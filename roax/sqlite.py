"""Module to manage resource items in a SQLite database."""

import contextlib
import logging
import roax.schema as s
import roax.db
import sqlite3
import threading


_logger = logging.getLogger(__name__)


class _CastAdapter:
    def __init__(self, type, sql_type):
        self.type = type
        self.sql_type = sql_type

    def sql_encode(self, schema, value):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve

    def sql_decode(self, schema, value):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve


class _PassAdapter:
    def __init__(self, sql_type):
        self.sql_type = sql_type

    def sql_encode(self, schema, value):
        return value

    def sql_decode(self, schema, value):
        return value


class _TextAdapter:

    sql_type = "TEXT"

    def sql_encode(self, schema, value):
        return schema.str_encode(value)

    def sql_decode(self, schema, value):
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


class Database(roax.db.Database):
    """
    Manages connections to a SQLite database.

    Parameter and instance variable:
    • file: Path to SQLite database file.

    Instance variable:
    • adapters: Column transformation adapters.
    """

    def __init__(self, file):
        super().__init__(sqlite3)
        self.file = file
        self.adapters = _adapters.copy()
        self._local = threading.local()

    @contextlib.contextmanager
    def connect(self):
        """
        Return a context manager that yields a database connection, providing
        transaction demarcation (commit/rollback on exit). Upon exit of the
        context manager, in the event of an exception, the transaction is
        rolled back; otherwise, the transaction is committed. 

        If more than one request for a connection is made in the same thread,
        the same connection will be yielded; only the outermost yielded
        connection shall exhibit transaction demarcation.
        """
        try:
            connection = self._local.connection
            self._local.count += 1
        except AttributeError:
            connection = sqlite3.connect(self.file)
            _logger.debug("%s", "sqlite connection begin")
            self._local.connection = connection
            self._local.count = 1
        try:
            yield connection
            if self._local.count == 1:
                _logger.debug("%s", "sqlite connection commit")
                connection.commit()
        except:
            if self._local.count == 1:
                _logger.debug("%s", "sqlite connection rollback")
                connection.rollback()
            raise
        finally:
            self._local.count -= 1
            if not self._local.count:
                del self._local.connection
