"""Module to manage resource items in a SQLite database."""

import aiosqlite
import contextlib
import contextvars
import fondat.schema as s
import fondat.sql
import logging


_logger = logging.getLogger(__name__)


class _CastAdapter:
    def __init__(self, type, sql_type):
        self.sql_type = sql_type
        self.type = type

    def sql_encode(self, value, schema):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve

    def sql_decode(self, value, schema):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve


class _PassAdapter:
    def __init__(self, sql_type):
        self.sql_type = sql_type

    def sql_encode(self, value, schema):
        return value

    def sql_decode(self, value, schema):
        return value


class _TextAdapter:

    sql_type = "TEXT"

    def sql_encode(self, value, schema):
        return schema.str_encode(value)

    def sql_decode(self, value, schema):
        return schema.str_decode(value)


INTEGER = _CastAdapter(int, "INTEGER")
REAL = _CastAdapter(float, "REAL")
TEXT = _TextAdapter()
BLOB = _PassAdapter("BLOB")


_adapters = {
    s.dataclass: TEXT,
    s.dict: TEXT,
    s.list: TEXT,
    s.set: TEXT,
    s.str: TEXT,
    s.int: INTEGER,
    s.float: REAL,
    s.bool: _CastAdapter(bool, "INTEGER"),
    s.bytes: BLOB,
    s.date: TEXT,
    s.datetime: TEXT,
    s.uuid: TEXT,
}


class Rows:
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


class Transaction:
    def __init__(self, database):
        self.database = database
        self.connection = None
        self.count = 0

    async def __aenter__(self):
        self.count += 1
        if not self.connection:
            self.connection = await aiosqlite.connect(self.database.file)
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

    def _sql_encode(self, param):
        value, schema = param
        if value is None or schema is None:
            return None
        return self.database.adapters[schema].sql_encode(value, schema)

    async def execute(self, statement):
        sql = "".join(
            map(lambda o: "?" if o is statement.PARAM else o, statement.operation)
        )
        params = [self._sql_encode(param) for param in statement.parameters]
        return Rows(await self.connection.execute(sql, params))


class Database(fondat.sql.Database):
    """
    Manages access to a SQLite database.

    Parameter:
    • file: Path to SQLite database file.
    """

    def __init__(self, file):
        super().__init__(_adapters)
        self.file = file
        self._tx = contextvars.ContextVar("fondat_sqlite_tx")

    @contextlib.asynccontextmanager
    async def transaction(self):
        """
        Return an asynchronous context manager that manages a database
        transaction.

        A transaction provides the means to execute queries and provides
        transaction semantics (commit/rollback). Upon exit of the context
        manager, in the event of an exception, the transaction will be
        rolled back; otherwise, the transaction will be committed. 

        If more than one request for a transaction is made within the same
        task, the same transaction will be returned; only the outermost
        yielded transaction will exhibit commit/rollback behavior.
        """
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
