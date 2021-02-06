"""Module to manage data in a SQLite database."""

from __future__ import annotations

import aiosqlite
import contextlib
import contextvars
import fondat.codec
import fondat.sql
import functools
import logging
import sqlite3
import typing

from asyncio.exceptions import CancelledError
from collections.abc import AsyncIterator
from fondat.codec import Codec, String
from fondat.types import affix_type_hints
from fondat.sql import Statement
from fondat.validation import validate_arguments
from typing import Annotated, Any, Literal, Optional, Union


_logger = logging.getLogger(__name__)

NoneType = type(None)


def _issubclass(cls, cls_or_tuple):
    try:
        return issubclass(cls, cls_or_tuple)
    except TypeError:
        return False


class SQLiteCodec(Codec[fondat.codec.F, Any]):
    """Base class for SQLite codecs."""


codec_providers = []


@functools.cache
def get_codec(python_type: Any) -> SQLiteCodec:
    """Return a codec compatible with the specified Python type."""

    if typing.get_origin(python_type) is Annotated:
        python_type = typing.get_args(python_type)[0]  # strip annotation

    for provider in codec_providers:
        if (codec := provider(python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide SQLite codec for {python_type}")


def _codec_provider(wrapped=None):
    if wrapped is None:
        return functools.partial(provider)
    codec_providers.append(wrapped)
    return wrapped


@_codec_provider
def _blob_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite BLOB. Supports the
    following types: bytes, bytearray.
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


@_codec_provider
def _integer_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite INTEGER. Supports the
    following types: int, bool.
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


@_codec_provider
def _real_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite REAL. Supports the
    following type: float.
    """

    if not _issubclass(python_type, float):
        return

    @affix_type_hints(localns=locals())
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
    Provides a codec that encodes/decodes a Union or Optional value to/from a compatible
    SQLite value. For Optional value, will use codec for its type, otherwise it
    encodes/decodes as TEXT.
    """

    origin = typing.get_origin(python_type)
    if origin is not Union:
        return

    args = typing.get_args(python_type)
    is_nullable = NoneType in args
    args = [a for a in args if a is not NoneType]
    codec = (
        get_codec(args[0])
        if len(args) == 1  # Optional[T]
        else _text_codec_provider(python_type)
    )

    @affix_type_hints(localns=locals())
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


@_codec_provider
def _literal_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a Literal value to/from a compatible SQLite value.
    If all literal values share the same type, then a codec for that type will be used,
    otherwise it encodes/decodes as TEXT.
    """

    origin = typing.get_origin(python_type)
    if origin is not Literal:
        return

    return get_codec(Union[tuple(type(arg) for arg in typing.get_args(python_type))])


@_codec_provider
def _text_codec_provider(python_type):
    """
    Provides a codec that encodes/decodes a value to/from a SQLite TEXT. It unconditionally
    returns the codec, regardless of Python type. It should be the last provider in the
    providers list to serve as a catch-all.
    """

    str_codec = fondat.codec.get_codec(String, python_type)

    @affix_type_hints(localns=locals())
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
    def __init__(self, statement, results):
        self.statement = statement
        self.results = results
        self.codecs = {
            k: get_codec(t)
            for k, t in typing.get_type_hints(statement.result, include_extras=True).items()
        }

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = await self.results.__anext__()
        return self.statement.result(**{k: self.codecs[k].decode(row[k]) for k in self.codecs})


class Database(fondat.sql.Database):
    """
    Manages access to a SQLite database.

    Parameter:
    • path: path to SQLite database file
    """

    def __init__(self, path: str):
        super().__init__()
        self.path = path
        self._connection = contextvars.ContextVar("fondat_sqlite_connection")

    @contextlib.asynccontextmanager
    async def transaction(self):

        connection = self._connection.get(None)
        token = None

        if not connection:
            _logger.debug("%s", "transaction begin")
            connection = await aiosqlite.connect(self.path)
            connection.row_factory = sqlite3.Row
            token = self._connection.set(connection)

        try:
            yield

        except Exception as e:

            # There is an issue in Python when a context manager is created
            # within a generator: if the generator is not iterated fully, the
            # context manager will not exit until the event loop cancels the
            # task by raising a CancelledError, long after the context is
            # assumed to be out of scope. Until there is some kind of fix,
            # this warning is an attempt to surface the problem.
            if type(e) is CancelledError:
                _logger.warning(
                    "%s",
                    "transaction failed due to CancelledError; "
                    "possible transaction context in aborted generator?",
                )

            # A GeneratorExit exception is raised when an explicit attempt
            # is made to cleanup an asynchronus generator via the aclose
            # coroutine method. Therefore, such an exception is not cause to
            # rollback the transaction.
            if token and not type(e) is GeneratorExit:
                _logger.debug("%s", "transaction rollback")
                await connection.rollback()
                raise

        else:
            if token:
                _logger.debug("%s", "transaction commit")
                await connection.commit()

        finally:
            if token:
                self._connection.reset(token)
                await connection.close()

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[Any]]:
        if not (connection := self._connection.get(None)):
            raise RuntimeError("transaction context required to execute statement")
        text = []
        args = []
        for fragment in statement:
            if isinstance(fragment, str):
                text.append(fragment)
            else:
                text.append("?")
                args.append(get_codec(fragment.python_type).encode(fragment.value))
        results = await connection.execute("".join(text), args)
        if statement.result is not None:  # expecting a result
            return _Results(statement, results.__aiter__())

    def get_codec(self, python_type: Any) -> SQLiteCodec:
        return get_codec(python_type)
