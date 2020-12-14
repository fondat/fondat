"""Module to manage resource items in a SQLite database."""

# from __future__ import annotations

import aiosqlite
import contextlib
import contextvars
import fondat.codec
import fondat.sql
import functools
import logging
import typing

from decimal import Decimal
from datetime import date, datetime
from fondat.sql import Statement
from typing import Annotated, Any, Union
from uuid import UUID


_logger = logging.getLogger(__name__)


class _CastAdapter:
    """Casts values to/from SQLite to Python type."""

    def __init__(self, pytype: type, sql_type: str):
        self.sql_type = sql_type
        self.pytype = pytype

    def sql_encode(self, value: Any) -> Any:
        return self.pytype(value)

    def sql_decode(self, value: Any) -> Any:
        return self.pytype(value)


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

    def __init__(self, pytype: type):
        self.sql_type = "TEXT"
        self.codec = fondat.codec.get_codec(pytype)

    def sql_encode(self, value: Any) -> Any:
        return self.codec.str_encode(value)

    def sql_decode(self, value: Any) -> Any:
        return self.codec.str_decode(value)


_adapters = {
    bool: _CastAdapter(bool, "INTEGER"),
    bytes: _PassAdapter("BLOB"),
    Decimal: _CastAdapter(float, "REAL"),
    float: _CastAdapter(float, "REAL"),
    int: _CastAdapter(int, "INTEGER"),
}


NoneType = type(None)


@functools.cache
def _get_adapter(pytype: type) -> Any:
    stripped = pytype
    if typing.get_origin(pytype) is Annotated:
        stripped = typing.get_args(pytype)[0]  # strip annotation
    if typing.get_origin(stripped) is Union:
        args = typing.get_args(stripped)
        if NoneType in args:
            return _get_adapter(Union[tuple(a for a in args if a is not NoneType)])
    if adapter := _adapters.get(stripped):
        return adapter
    else:
        return _TextAdapter(pytype)


class Results:
    def __init__(self, cursor):
        self.cursor = cursor

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.cursor.close()

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.cursor.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


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
        return await aiosqlite.connect(self.path)

    def sql_encode(self, pytype: type, value: Any) -> Any:
        if value is not None:
            return _get_adapter(pytype).sql_encode(value)

    def sql_decode(self, pytype: type, value: Any) -> Any:
        if value is not None:
            return _get_adapter(pytype).sql_decode(value)

    def sql_type(self, pytype: type):
        return _get_adapter(pytype).sql_type


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

    async def execute(self, statement: Statement) -> Results:
        text = []
        params = []
        for fragment in statement:
            if isinstance(fragment, str):
                text.append(fragment)
            else:
                text.append("?")
                params.append(self.database.sql_encode(fragment.pytype, fragment.value))
        return Results(await self.connection.execute("".join(text), params))
