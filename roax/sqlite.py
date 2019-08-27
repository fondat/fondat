"""Module to manage resource items in a SQLite database."""

import roax.schema as s
import roax.db as db
import sqlite3


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

    def connect(self):
        """
        Return a context manager that yields a database connection with transaction demarcation.
        If more than one request for a connection is made in the same thread, the same connection
        may be returned; only the outermost yielded connection shall have transaction demarcation.
        """
        return sqlite3.connect(self.file)


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
