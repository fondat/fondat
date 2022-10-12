"""Module to manage data in a SQLite database."""

import aiosqlite
import asyncio
import contextvars
import fondat.codec
import fondat.sql
import logging
import sqlite3
import types
import typing
import uuid

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fondat.codec import Codec, DecodeError, EncodeError
from fondat.sql import Expression, Param
from fondat.types import is_optional, is_subclass, literal_values, strip_annotations
from types import NoneType
from typing import Any, Literal, TypeVar


_logger = logging.getLogger(__name__)


T = TypeVar("T")
PT = TypeVar("PT")  # Python type hint
ST = TypeVar("ST")  # SQL type hint


class SQLiteCodec(Codec[PT, ST]):
    """Base class for SQLite codecs."""

    _cache = {}


class BlobCodec(SQLiteCodec[bytes | bytearray, bytes]):
    """
    Codec that encodes/decodes a value to/from a SQL BLOB. Supports the following types:
    bytes, bytearray.
    """

    sql_type = "BLOB"

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, bytes | bytearray)

    def encode(self, value: bytes | bytearray) -> bytes:
        if not isinstance(value, bytes | bytearray):
            raise EncodeError
        return bytes(value)

    def decode(self, value: bytes) -> bytes | bytearray:
        if not isinstance(value, bytes):
            raise DecodeError
        return self.python_type(value)


class IntegerCodec(SQLiteCodec[int | bool, int]):
    """
    Codec that encodes/decodes a value to/from a SQL INTEGER. Supports the following types:
    int, bool.
    """

    sql_type = "INTEGER"

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, int | bool)

    def encode(self, value: int | bool) -> int:
        if not isinstance(value, int | bool):
            raise EncodeError
        return int(value)

    def decode(self, value: int) -> int | bool:
        if not isinstance(value, int):
            raise DecodeError
        return self.python_type(value)


class RealCodec(SQLiteCodec[float, float]):
    """
    Codec that encodes/decodes a value to/from a SQL REAL. Supports the following type: float.
    """

    sql_type = "REAL"

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, float)

    def encode(self, value: float) -> float:
        if not isinstance(value, float):
            raise EncodeError
        return value

    def decode(self, value: float) -> float:
        if not isinstance(value, float):
            raise DecodeError
        return value


class UnionCodec(SQLiteCodec[PT, Any]):
    """
    Codec that encodes/decodes a UnionType, Union or optional value to/from a compatible SQL
    value. For an optional type, it will use the codec for its type, otherwise it will
    encode/decode as TEXT.
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return typing.get_origin(python_type) in {typing.Union, types.UnionType}

    def __init__(self, python_type: type[PT]):
        super().__init__(python_type)
        raw_type = strip_annotations(python_type)
        args = typing.get_args(raw_type)
        self.is_nullable = is_optional(raw_type)
        args = [a for a in args if a is not NoneType]
        self.codec = SQLiteCodec.get(args[0]) if len(args) == 1 else TextCodec(python_type)
        self.sql_type = self.codec.sql_type

    def encode(self, value: PT) -> Any:
        if value is None:
            return None
        return self.codec.encode(value)

    def decode(self, value: Any) -> PT:
        if value is None and self.is_nullable:
            return None
        return self.codec.decode(value)


class LiteralCodec(SQLiteCodec[PT, Any]):
    """
    Codec that encodes/decodes a Literal value to/from a compatible SQL value. If all literal
    values share the same type, then it will use a codec for that type, otherwise it will
    encode/decode as TEXT.
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return typing.get_origin(python_type) is Literal

    def __init__(self, python_type: type[PT]):
        super().__init__(python_type)
        self.literals = literal_values(python_type)
        types = list({type(literal) for literal in self.literals})
        self.codec = SQLiteCodec.get(types[0]) if len(types) == 1 else TextCodec(python_type)
        self.is_nullable = is_optional(python_type) or None in self.literals
        self.sql_type = self.codec.sql_type

    def encode(self, value: PT) -> Any:
        if value is None:
            return None
        return self.codec.encode(value)

    def decode(self, value: Any) -> PT:
        if value is None and self.is_nullable:
            return None
        result = self.codec.decode(value)
        if result not in self.literals:
            raise DecodeError
        return result


class TextCodec(SQLiteCodec[PT, Any]):
    """
    Codec that encodes/decodes a value to/from a SQL TEXT. This is the "fallback" codec,
    which handles any type not handled by any other codec.
    """

    sql_type = "TEXT"

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        for other in (c for c in SQLiteCodec.__subclasses__() if c is not TextCodec):
            if other.handles(python_type):
                return False
        return True

    def __init__(self, python_type):
        super().__init__(python_type)
        self.string_codec = fondat.codec.StringCodec.get(python_type)

    def encode(self, value: PT) -> str:
        return self.string_codec.encode(value)

    def decode(self, value: str) -> PT:
        return self.string_codec.decode(value)


class _Results(AsyncIterator[Any]):

    __slots__ = {"statement", "result", "rows", "codecs"}

    def __init__(self, statement, result, rows):
        self.statement = statement
        self.result = result
        self.rows = rows
        self.codecs = {
            k: SQLiteCodec.get(t)
            for k, t in typing.get_type_hints(result, include_extras=True).items()
        }

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.rows.__anext__()
        return self.result(**{k: self.codecs[k].decode(row[k]) for k in self.codecs})


@asynccontextmanager
async def _async_null_context():
    yield


class Database(fondat.sql.Database):
    """
    Manages access to a SQLite database.

    Parameter:
    • path: path to SQLite database file
    """

    __slots__ = {"path", "_conn", "_txn"}

    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self._conn = contextvars.ContextVar("fondat_sqlite_conn", default=None)
        self._txn = contextvars.ContextVar("fondat_sqlite_txn", default=None)
        self._task = contextvars.ContextVar("fondat_sqlite_task", default=None)

    @asynccontextmanager
    async def connection(self):
        task = asyncio.current_task()
        if self._conn.get() and self._task.get() is task:
            yield  # connection already established
            return
        _logger.debug("open connection")
        self._task.set(task)
        connection = await aiosqlite.connect(self.path)
        connection.row_factory = sqlite3.Row
        self._conn.set(connection)
        try:
            yield
        finally:
            _logger.debug("close connection")
            self._conn.set(None)
            try:
                await connection.close()
            except Exception as e:
                _logger.exception("error closing connection")

    @asynccontextmanager
    async def transaction(self):
        txid = f"_{uuid.uuid4().hex}"
        _logger.debug("transaction begin %s", txid)
        token = self._txn.set(txid)

        async def commit():
            _logger.debug("transaction commit %s", txid)
            await connection.execute(f"RELEASE SAVEPOINT {txid};")

        async def rollback():
            _logger.debug("transaction rollback %s", txid)
            await connection.execute(f"ROLLBACK TO SAVEPOINT {txid};")

        async with self.connection():
            connection = self._conn.get()
            await connection.execute(f"SAVEPOINT {txid};")
            try:
                yield
            except GeneratorExit:  # explicit cleanup of asynchronous generator
                await commit()
            except Exception:
                await rollback()
                raise
            else:
                await commit()
            finally:
                self._txn.reset(token)

    async def execute(
        self,
        statement: Expression,
        result: type = None,
    ) -> AsyncIterator[Any] | None:
        if not self._txn.get():
            raise RuntimeError("transaction context required to execute statement")
        text = []
        args = []
        for fragment in statement:
            match fragment:
                case str():
                    text.append(fragment)
                case Param():
                    text.append("?")
                    args.append(SQLiteCodec.get(fragment.type).encode(fragment.value))
                case _:
                    raise ValueError(f"unexpected fragment: {fragment}")
        results = await self._conn.get().execute("".join(text), args)
        if result is not None:  # expecting a result
            return _Results(statement, result, results.__aiter__())

    def sql_type(self, type: Any) -> str:
        return SQLiteCodec.get(type).sql_type


class Table(fondat.sql.Table[T]):
    """..."""

    async def upsert(self, value: T):
        """
        Upsert table row. Must be called within a database transaction context.
        """
        stmt = Expression(
            f"INSERT INTO {self.name} (",
            ", ".join(self.columns),
            ") VALUES (",
            Expression.join(
                (
                    Param(getattr(value, name), python_type)
                    for name, python_type in self.columns.items()
                ),
                ", ",
            ),
            f") ON CONFLICT ({self.pk}) DO UPDATE SET ",
            Expression.join(
                (
                    Expression(f"{name} = ", Param(getattr(value, name), python_type))
                    for name, python_type in self.columns.items()
                    if name != self.pk
                ),
                ", ",
            ),
            ";",
        )
        await self.database.execute(stmt)
