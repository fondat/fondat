"""Module to manage resource items in a SQLite database."""

import contextlib
import logging
import roax.schema as s
import roax.db as db
import sqlite3
import threading


_logger = logging.getLogger(__name__)


class _CastCodec:
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


class _TextCodec:
    def encode(self, schema, value):
        return schema.str_encode(value)

    def decode(self, schema, value):
        return schema.str_decode(value)


class _PassCodec:
    def encode(self, schema, value):
        return value

    def decode(self, schema, value):
        return value


INTEGER = _CastCodec(int)
REAL = _CastCodec(float)
TEXT = _TextCodec()
BLOB = _PassCodec()
BOOLEAN = _CastCodec(bool)


_codecs = {
    s.dict: TEXT,
    s.list: TEXT,
    s.set: TEXT,
    s.int: INTEGER,
    s.float: REAL,
    s.bool: BOOLEAN,
    s.bytes: BLOB,
    s.date: TEXT,
    s.datetime: TEXT,
    s.uuid: TEXT,
}


class Database(db.Database):
    """TODO: Description."""

    def __init__(self, file):
        super().__init__(sqlite3)
        self.file = file
        self.local = threading.local()

    @contextlib.contextmanager
    def connect(self):
        """
        Return a context manager that yields a database connection with transaction demarcation.
        If more than one request for a connection is made in the same thread, the same connection
        will be returned; only the outermost yielded connection shall have transaction demarcation.
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


class Table(db.Table):
    """TODO: Description."""

    def __init__(self, name, schema, pk, codecs=None):
        """
        :param module: Module that implements the DB-API interface.
        :param name: Name of table in the SQL database.
        :param schema: Schema of table columns.
        :param primary_key: Column name of the primary key.
        :param codecs: TODO.
        """
        super().__init__(name, schema, pk, {**_codecs, **(codecs if codecs else {})})
