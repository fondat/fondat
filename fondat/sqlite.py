"""Module to manage data in a SQLite database."""

import aiosqlite
import asyncio
import contextvars
import fondat.codec
import fondat.sql
import functools
import logging
import sqlite3
import types
import typing
import uuid

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from fondat.codec import Codec, DecodeError, String
from fondat.sql import Expression, Param
from fondat.types import is_optional, is_subclass, literal_values, split_annotated
from fondat.validation import validate_arguments
from types import NoneType
from typing import Any, Literal


_logger = logging.getLogger(__name__)


class SQLiteCodec(Codec[fondat.codec.F, Any]):
    """Base class for SQLite codecs."""


codec_providers = []


@functools.cache
def get_codec(python_type: Any) -> SQLiteCodec:
    """Return a codec compatible with the specified Python type."""

    python_type, _ = split_annotated(python_type)

    for provider in codec_providers:
        if (codec := provider(python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide SQLite codec for {python_type}")


def _codec_provider(wrapped=None):
    if wrapped is None:
        return functools.partial(_codec_provider)
    codec_providers.append(wrapped)
    return wrapped


@_codec_provider
def _blob_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite BLOB. Supports the
    following types: bytes, bytearray.
    """

    if not is_subclass(python_type, (bytes, bytearray)):
        return

    class BlobCodec(SQLiteCodec[python_type]):

        sql_type = "BLOB"

        @validate_arguments
        def encode(self, value: python_type) -> bytes:
            return bytes(value)

        @validate_arguments
        def decode(self, value: bytes) -> python_type:
            return python_type(value)

    return BlobCodec()


@_codec_provider
def _integer_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite INTEGER. Supports the
    following types: int, bool.
    """

    if not is_subclass(python_type, int):  # includes bool
        return

    class IntegerCodec(SQLiteCodec[python_type]):

        sql_type = "INTEGER"

        @validate_arguments
        def encode(self, value: python_type) -> int:
            return int(value)

        @validate_arguments
        def decode(self, value: int) -> python_type:
            return python_type(value)

    return IntegerCodec()


@_codec_provider
def _real_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite REAL. Supports the
    following type: float.
    """

    if not is_subclass(python_type, float):
        return

    class RealCodec(SQLiteCodec[python_type]):

        sql_type = "REAL"

        @validate_arguments
        def encode(self, value: python_type) -> float:
            return float(value)

        @validate_arguments
        def decode(self, value: float) -> python_type:
            return python_type(value)

    return RealCodec()


@_codec_provider
def _union_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a UnionType, Union or Optional value to/from a
    compatible SQLite value. For an optional type, it will use the codec for its type,
    otherwise it will encode/decode as TEXT.
    """

    origin = typing.get_origin(python_type)
    if origin not in {typing.Union, types.UnionType}:
        return

    args = typing.get_args(python_type)
    is_nullable = is_optional(python_type)
    args = [a for a in args if a is not NoneType]
    codec = get_codec(args[0]) if len(args) == 1 else _text_codec_provider(python_type)

    class UnionCodec(SQLiteCodec[python_type]):

        sql_type = codec.sql_type

        @validate_arguments
        def encode(self, value: python_type) -> Any:
            if value is None:
                return None
            return codec.encode(value)

        def decode(self, value: Any) -> python_type:
            if value is None and is_nullable:
                return None
            return codec.decode(value)

    return UnionCodec()


@_codec_provider
def _literal_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a Literal value to/from a compatible SQLite value.
    If all literal values share the same type, then it will use a codec for that type,
    otherwise it will encode/decode as TEXT.
    """

    origin = typing.get_origin(python_type)

    if origin is not Literal:
        return

    literals = literal_values(python_type)
    types = list({type(literal) for literal in literals})
    codec = get_codec(types[0]) if len(types) == 1 else _text_codec_provider(python_type)
    is_nullable = is_optional(python_type) or None in literals

    class LiteralCodec(SQLiteCodec[python_type]):

        sql_type = codec.sql_type

        @validate_arguments
        def encode(self, value: python_type) -> Any:
            if value is None:
                return None
            return codec.encode(value)

        def decode(self, value: Any) -> python_type:
            if value is None and is_nullable:
                return None
            result = codec.decode(value)
            if result not in literals:
                raise DecodeError
            return result

    return LiteralCodec()


@_codec_provider
def _text_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite TEXT. It unconditionally
    returns the codec, regardless of Python type. It should be the last provider in the
    providers list to serve as a catch-all.
    """

    str_codec = fondat.codec.get_codec(String, python_type)

    class TextCodec(SQLiteCodec):

        sql_type = "TEXT"

        @validate_arguments
        def encode(self, value: python_type) -> str:
            return str_codec.encode(value)

        @validate_arguments
        def decode(self, value: str) -> python_type:
            return str_codec.decode(value)

    return TextCodec()


class _Results(AsyncIterator[Any]):

    __slots__ = {"statement", "result", "rows", "codecs"}

    def __init__(self, statement, result, rows):
        self.statement = statement
        self.result = result
        self.rows = rows
        self.codecs = {
            k: get_codec(t)
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
                    args.append(get_codec(fragment.type).encode(fragment.value))
                case _:
                    raise ValueError(f"unexpected fragment: {fragment}")
        results = await self._conn.get().execute("".join(text), args)
        if result is not None:  # expecting a result
            return _Results(statement, result, results.__aiter__())

    def sql_type(self, type: Any) -> str:
        return get_codec(type).sql_type
