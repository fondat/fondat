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
from datetime import date, datetime
from fondat.codec import Codec, String
from fondat.types import affix_type_hints
from fondat.sql import Statement
from fondat.validate import validate_arguments
import sqlite3
from typing import Annotated, Any, Literal, Union
from uuid import UUID


_logger = logging.getLogger(__name__)

NoneType = type(None)


codec_providers = []


def get_codec(python_type):

    if typing.get_origin(python_type) is Annotated:
        python_type = typing.get_args(python_type)[0]  # strip annotation

    for provider in code_providers:
        if (codec := provider(python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide {codec_type} for {python_type}")


def _issubclass(cls, cls_or_tuple):
    try:
        return issubclass(cls, cls_or_tuple)
    except TypeError:
        return False


class SQLiteCodec(Codec[fondat.codec.F, Any]):
    pass


# TODO: enum_codec


def blob_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite BLOB.
    Supports the following types: bytes, bytearray.
    """

    if not _issubclass(python_type, (bytes, bytearray)):
        return

    @affix_type_hints(localns=locals())
    class BlobCodec(SQLiteCodec[python_type]):

        sql_type = "BLOB"

        @validate_arguments
        def encode(self, value: python_type) -> bytes:
            return bytes(value)

        @validate_arguments
        def decode(self, value: bytes) -> python_type:
            return python_type(value)

    return BlobCodec()


def integer_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite INTEGER.
    Supports the following types: int, bool.
    """

    if not _issubclass(python_type, int):  # includes bool
        return

    @affix_type_hints(localns=locals())
    class IntegerCodec(SQLiteCodec[python_type]):

        sql_type = "INTEGER"

        @validate_arguments
        def encode(self, value: python_type) -> int:
            return int(value)

        @validate_arguments
        def decode(self, value: int) -> python_type:
            return python_type(value)

    return IntegerCodec()


def real_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite REAL.
    Supports the following type: float.
    """

    if not _issubclass(python_type, float):
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


def union_codec(python_type):
    """
    Provides a codec that encodes/decodes a Union or Optional value to/from a
    compatible SQLite value. For Optional value, will use codec for its type,
    otherwise it encodes/decodes as TEXT.
    """

    origin = typing.get_origin(python_type)
    if origin is not Union:
        return

    args = typing.get_args(python_type)
    is_nullable = NoneType in args
    args = [a for a in args if a is not NoneType]
    codec = get_codec(args[0]) if len(args) == 1 else text_provider(python_type)

    class UnionCodec(SQLiteCodec[python_type]):

        sql_type = codec.sql_type

        @validate_arguments
        def encode(self, value: python_type) -> Any:
            if value is None:
                return None
            return codec.encode(value)

        @validate_arguments
        def decode(self, value: Any) -> python_type:
            if value is None and is_nullable:
                return None
            return codec.decode(value)

    return UnionCodec()


def literal_provider(python_type):
    """
    Provides a codec that encodes/decodes a Literal value to/from a compatible
    SQLite value. If all literal values share the same type, then a code for
    that type will be used, otherwise it encodes/decodes as TEXT.
    """

    origin = typing.get_origin(python_type)
    if origin is not Literal:
        return

    return get_codec(Union[tuple(type(arg) for arg in typing.get_args(python_type))])


def text_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite TEXT. It
    unconditionally returns the codec, regardless of Python type. It should be
    the last provider in the list to serve as a catch-all.
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


providers = [
    blob_provider,
    integer_provider,
    real_provider,
    union_codec,
    literal_provider,
    text_provider,  # intentionally last
]


@functools.cache
def get_codec(python_type):
    """Return a codec compatible with the specified Python type."""

    if typing.get_origin(python_type) is Annotated:
        python_type = typing.get_args(python_type)[0]  # strip annotation

    for provider in providers:
        if (codec := provider(python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide SQLite codec for {python_type}")


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

    def get_codec(self, python_type: Any) -> SQLiteCodec:
        return get_codec(python_type)


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
                args.append(
                    self.database.get_codec(fragment.py_type).encode(fragment.value)
                )
        return "".join(text), args

    async def execute(self, statement: Statement) -> None:
        """Execute a statement with no expected result."""
        await self.connection.execute(*self._stmt(statement))

    async def query(self, query: Statement) -> AsyncIterable[Mapping[str, Any]]:
        """Execute a statement with an expected result."""
        return await self.connection.execute(*self._stmt(query))
