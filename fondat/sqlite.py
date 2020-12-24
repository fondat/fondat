"""Module to manage resource items in a SQLite database."""

# from __future__ import annotations

import aiosqlite
import contextlib
import contextvars
import enum
import fondat.codec
import fondat.sql
import functools
import logging
import typing

from collections.abc import AsyncIterable, Iterable, Mapping
from decimal import Decimal
from datetime import date, datetime
from fondat.sql import Statement
import sqlite3
from typing import Annotated, Any, Union
from uuid import UUID


_logger = logging.getLogger(__name__)


class _CastAdapter:
    """Casts values to/from SQLite to Python type."""

    def __init__(self, py_type: type, sql_type: str):
        self.sql_type = sql_type
        self.py_type = py_type

    def sql_encode(self, value: Any) -> Any:
        return self.py_type(value)

    def sql_decode(self, value: Any) -> Any:
        return self.py_type(value)


class _PassAdapter:
    """Passes values to/from SQLite verbatim."""

    def __init__(self, sql_type: str):
        self.sql_type = sql_type

    def sql_encode(self, value: Any) -> Any:
        return value

    def sql_decode(self, value: Any) -> Any:
        return value


class _TextAdapter:
    """Converts values to/from SQLite to strings."""

    def __init__(self, py_type: type):
        self.sql_type = "TEXT"
        self.codec = fondat.codec.get_codec(py_type)

    def sql_encode(self, value: Any) -> Any:
        return self.codec.str_encode(value)

    def sql_decode(self, value: Any) -> Any:
        return self.codec.str_decode(value)


class _EnumAdapter:
    """Converts value to/from SQLite to enumerations."""

    def __init__(self, py_type: type):
        self._py_type = py_type
        self._utype = Union[tuple(type(member.value) for member in py_type)]
        if typing.get_origin(self._utype) is Union:
            raise TypeError("SQLite does not support mixed-type Enums")
        self._adapter = _get_adapter(self._utype)
        self.sql_type = self._adapter.sql_type

    def sql_encode(self, value: Any) -> Any:
        return self._adapter.sql_encode(value.value)

    def sql_decode(self, value: Any) -> Any:
        return self._py_type(self._adapter.sql_decode(value))


_adapters = {
    bool: _CastAdapter(bool, "INTEGER"),
    bytes: _PassAdapter("BLOB"),
    Decimal: _CastAdapter(float, "REAL"),
    float: _CastAdapter(float, "REAL"),
    int: _CastAdapter(int, "INTEGER"),
}


NoneType = type(None)


def _issubclass(cls, cls_or_tuple):
    try:
        return issubclass(cls, cls_or_tuple)
    except TypeError:
        return False


@functools.cache
def _get_adapter(py_type: type) -> Any:
    stripped = py_type
    if typing.get_origin(py_type) is Annotated:
        stripped = typing.get_args(py_type)[0]  # strip annotation
    if typing.get_origin(stripped) is Union:
        args = typing.get_args(stripped)
        if NoneType in args:
            return _get_adapter(Union[tuple(a for a in args if a is not NoneType)])
    if adapter := _adapters.get(stripped):
        return adapter
    elif _issubclass(py_type, enum.Enum):
        return _EnumAdapter(py_type)
    else:
        return _TextAdapter(py_type)


class Database(fondat.sql.Database):
    """
    Manages access to a SQLite database.

    Parameter:
    • path: Path to SQLite database file.
    """

    def __init__(self, path):
        super().__init__()
        self.path = path
        self._tx = contextvars.ContextVar("fondat_sqlite_tx")

    @contextlib.asynccontextmanager
    async def transaction(self):
        t = self._tx.get(None)
        token = None
        if not t:
            t = Transaction(self)
            token = self._tx.set(t)
        try:
            async with t:
                yield t
        finally:
            if token:
                self._tx.reset(token)

    async def connect(self):
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def sql_encode(self, py_type: type, value: Any) -> Any:
        if value is not None:
            return _get_adapter(py_type).sql_encode(value)

    def sql_decode(self, py_type: type, value: Any) -> Any:
        if value is not None:
            return _get_adapter(py_type).sql_decode(value)

    def sql_type(self, py_type: type):
        return _get_adapter(py_type).sql_type


class Transaction(fondat.sql.Transaction):
    def __init__(self, database: Database):
        super().__init__()
        self.database = database
        self.connection = None
        self.count = 0

    async def __aenter__(self):
        self.count += 1
        if not self.connection:
            self.connection = await self.database.connect()
            _logger.debug("%s", "transaction begin")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.count -= 1
        if self.count <= 0:
            if not exc_type:
                await self.connection.commit()
                _logger.debug("%s", "transaction commit")
            else:
                await self.connection.rollback()
                _logger.debug("%s", "transaction rollback")
            await self.connection.close()
            self.connection = None
            self.count = 0

    def _stmt(self, statement: Statement) -> tuple[str, Iterable[Any]]:
        text = []
        args = []
        for fragment in statement:
            if isinstance(fragment, str):
                text.append(fragment)
            else:
                text.append("?")
                args.append(self.database.sql_encode(fragment.py_type, fragment.value))
        return "".join(text), args

    async def execute(self, statement: Statement) -> None:
        """Execute a statement with no expected result."""
        await self.connection.execute(*self._stmt(statement))

    async def query(self, query: Statement) -> AsyncIterable[Mapping[str, Any]]:
        """Execute a statement with an expected result."""
        return await self.connection.execute(*self._stmt(query))
