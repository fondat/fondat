"""Module to manage resource items in a SQLite database."""

import contextlib
import logging
import roax.schema as s
import roax.db as db
import sqlite3
import threading


_logger = logging.getLogger(__name__)


class _CastAdapter:
    def __init__(self, type):
        self.type = type

    def encode(self, schema, value):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve

    def decode(self, schema, value):
        try:
            return self.type(value)
        except ValueError as ve:
            raise s.SchemaError(str(ve)) from ve


class _PassAdapter:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return value


INTEGER = _CastAdapter(int)
REAL = _CastAdapter(float)
TEXT = db.default_adapter
BLOB = _PassAdapter()
BOOLEAN = _CastAdapter(bool)


_adapters = {
    s.dict: TEXT,
    s.list: TEXT,
    s.set: TEXT,
    s.str: TEXT,
    s.int: INTEGER,
    s.float: REAL,
    s.bool: BOOLEAN,
    s.bytes: BLOB,
    s.date: TEXT,
    s.datetime: TEXT,
    s.uuid: TEXT,
}


class Database(db.Database):
    """Manages connections to a SQLite database."""

    def __init__(self, file):
        super().__init__(sqlite3)
        self.file = file
        self.local = threading.local()
        self.adapters = _adapters

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
            connection = self.local.connection
            self.local.count += 1
        except AttributeError:
            connection = sqlite3.connect(self.file)
            _logger.debug("%s", "sqlite connection begin")
            self.local.connection = connection
            self.local.count = 1
        try:
            yield connection
            if self.local.count == 1:
                _logger.debug("%s", "sqlite connection commit")
                connection.commit()
        except:
            if self.local.count == 1:
                _logger.debug("%s", "sqlite connection rollback")
                connection.rollback()
            raise
        finally:
            self.local.count -= 1
            if not self.local.count:
                del self.local.connection
