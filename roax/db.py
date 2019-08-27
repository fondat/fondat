"""Module to manage resource items in a SQL database through DB-API 2.0 interface."""

import roax.schema as s

from .resource import BadRequest, NotFound, InternalServerError, Resource
from contextlib import contextmanager


@contextmanager
def _nullcontext(value):
    yield value


_markers = {
    "qmark": "?",
    "numeric": ":{}",
    "named": ":{}",
    "format": "%s",
    "pyformat": "%({})s",
}


class _default_codec:
    def encode(self, schema, value):
        return schema.str_encode(value)

    def decode(self, schema, value):
        return schema.str_decode(value)


class Database:
    """TODO: Description."""

    def __init__(self, module):
        self.module = module
        self.paramstyle = module.paramstyle

    def connect(self):
        """
        Return a context manager that yields a database connection with transaction demarcation.
        If more than one request for a connection is made in the same thread, the same connection
        may be returned; only the outermost yielded connection shall have transaction demarcation.
        """
        raise NotImplementedError

    @contextmanager
    def cursor(self, connection=None):
        """Return a context manager that yields a cursor that automatically closes."""
        with _nullcontext(connection) if connection else self.connect() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def query(self):
        """Return a new query builder for the resource database."""
        return Query(self)


class Table:
    """TODO: Description."""

    def __init__(self, name, schema, pk, codecs):
        """
        :param module: Module that implements the DB-API interface.
        :param name: Name of table in the SQL database.
        :param schema: Schema of table columns.
        :param primary_key: Column name of the primary key.
        """
        if not isinstance(schema, s.dict):
            raise ValueError("schema must be dict")
        super().__init__()
        self.name = name
        self.schema = schema
        self.pk = pk
        self.codecs = codecs

    @property
    def columns(self):
        return self.schema.properties.keys()

    def _call_codec(self, method, column, value):
        schema = self.schema.properties[column]
        if column in self.codecs:
            codec = self.codecs[column]
        elif schema.__class__ in self.codecs:
            codec = self.codecs[schema.__class__]
        else:
            codec = _default_codec
        return codec.__getattribute__(method)(schema, value)

    def encode(self, column, value):
        return self._call_codec("encode", column, value)

    def decode(self, column, value):
        return self._call_codec("decode", column, value)


class Query:
    """Builds queries that manage text and parameters."""

    def __init__(self, database):
        """TODO: Description."""
        self.database = database
        self._query = []
        self._params = []

    def text(self, value):
        """TODO: Description."""
        self._query.append(value)
        return self

    def param(self, value):
        """TODO: Description."""
        self._query.append(_markers[self.database.paramstyle].format(len(self._params)))
        self._params.append(value)
        return self

    def params(self, values, sep=", "):
        """TODO: Description."""
        for n in range(0, len(values)):
            self.param(values[n])
            if n < len(values) - 1:
                self.text(sep)
        return self

    def build(self):
        """TODO: Description."""
        query = "".join(self._query)
        if self.database.paramstyle in {"named", "pyformat"}:
            params = {str(n): self._params[n] for n in range(0, len(self._params))}
        else:
            params = self._params
        return (query, params)


class TableResource(Resource):
    """
    Base resource class for storage of resource items in a database table.
    """

    def __init__(self, database, table, name=None, description=None):
        """
        Initialize table resource.

        :param database: TODO.
        :param table TODO.
        :param name: Short name of the resource.  [table.name]
        :param description: Short description of the resource.  [resource docstring]
        """
        super().__init__(name or table.name, description)
        self.database = database or self.database
        self.table = table or self.database
        self.__doc__ = self.table.schema.description

    def connect(self):
        """
        Return a context manager that yields a database connection with transaction demarcation.
        If more than one request for a connection is made in the same thread, the same connection
        may be returned; only the outermost yielded connection shall have transaction demarcation.
        """
        return self.database.connect()

    @contextmanager
    def cursor(self, connection=None):
        """Return a context manager that yields a cursor that automatically closes."""
        with _nullcontext(connection) if connection else self.connect() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def query(self):
        """Return a new query builder for the resource database."""
        return self.database.query()

    def create(self, id, _body):
        query = self.query()
        query.text(f"INSERT INTO {self.table.name} (")
        query.text(", ".join(self.table.columns))
        query.text(") VALUES (")
        query.params(
            [
                self.table.encode(column, _body.get(column))
                for column in self.table.columns
            ]
        )
        query.text(");")
        with self.cursor() as cursor:
            cursor.execute(*query.build())

    def read(self, id):
        query = self.query()
        query.text("SELECT ")
        query.text(", ".join(self.table.columns))
        query.text(f" FROM {self.table.name}")
        query.text(f" WHERE {self.table.pk} = ")
        query.param(self.table.encode(self.table.pk, id))
        query.text(";")
        with self.cursor() as cursor:
            cursor.execute(*query.build())
            fetched = cursor.fetchone()
            if fetched is None:
                raise NotFound()
            if cursor.fetchone():
                raise InternalServerError("query matches more than one row")
            result = dict(zip(self.table.columns, fetched))
        for column in self.table.columns:
            if result[column] is None and column not in self.table.schema.required:
                del result[column]
            else:
                result[column] = self.table.decode(column, result[column])
        self.table.schema.validate(result)
        return result

    def update(self, id, _body):
        self.table.schema.validate(_body)
        query = self.query()
        query.text(f"UPDATE {self.table.name} SET ")
        columns = tuple(self.table.columns)
        for n in range(0, len(columns)):
            column = columns[n]
            query.text(f"{column} = ")
            value = (
                self.table.encode(column, _body[column]) if column in _body else None
            )
            query.param(value)
            if n < len(columns) - 1:
                query.text(", ")
        query.text(f" WHERE {self.table.pk} = ")
        query.param(self.table.encode(self.table.pk, id))
        query.text(";")
        with self.cursor() as cursor:
            cursor.execute(*query.build())
            if cursor.rowcount == 0:
                raise NotFound()
            elif cursor.rowcount > 1:
                raise InternalServerError("query matches more than one row")
            elif cursor.rowcount == -1:
                raise InternalServerError("could not determine update was successful")

    def delete(self, id):
        query = self.query()
        query.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
        query.param(self.table.encode(self.table.pk, id))
        query.text(";")
        with self.cursor() as cursor:
            cursor.execute(*query.build())
            if cursor.rowcount < 1:
                raise NotFound()
            elif cursor.rowcount > 1:
                raise InternalServerError("query would delete more than one row")
