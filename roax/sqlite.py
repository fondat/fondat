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

    def __init__(self, database, name, schema, pk, adapters=None):
        """
        :param database: Database where table resides.
        :param name: Name of database table.
        :param schema: Schema of table columns.
        :param pk: Column name of the primary key.
        :param adapters: Column transformation adapters.
        """
        super().__init__(
            database, name, schema, pk, {**_adapters, **(adapters if adapters else {})}
        )
