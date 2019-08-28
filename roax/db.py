"""Module to manage resource items in a SQL database through DB-API 2.0 interface."""

import contextlib
import logging
import roax.schema as s

from roax.resource import BadRequest, NotFound, InternalServerError, Resource

_logger = logging.getLogger(__name__)


@contextlib.contextmanager
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
        will be returned; only the outermost yielded connection shall have transaction demarcation.
        """
        raise NotImplementedError

    @contextlib.contextmanager
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
        return list(self.schema.properties.keys())

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
        return self._call_codec("encode", column, value) if value is not None else None

    def decode(self, column, value):
        return self._call_codec("decode", column, value) if value is not None else None


class Query:
    """Builds queries that manage text and parameters."""

    def __init__(self, database):
        """TODO: Description."""
        self.database = database
        self._operation = []
        self._parameters = []

    def text(self, value):
        """TODO: Description."""
        self._operation.append(value)

    def param(self, value):
        """TODO: Description."""
        self._operation.append(
            _markers[self.database.paramstyle].format(len(self._parameters))
        )
        self._parameters.append(value)

    def params(self, values, sep=", "):
        """TODO: Description."""
        for n in range(0, len(values)):
            self.param(values[n])
            if n < len(values) - 1:
                self.text(sep)

    def query(self, query):
        self._operation += query._operation
        self._parameters += query._parameters

    def build(self):
        """TODO: Description."""
        operation = "".join(self._operation)
        if self.database.paramstyle in {"named", "pyformat"}:
            parameters = {
                str(n): self._parameters[n] for n in range(0, len(self._parameters))
            }
        else:
            parameters = self._parameters
        return (operation, parameters)


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

    @contextlib.contextmanager
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

    @contextlib.contextmanager
    def execute(self, query, connection=None):
        """TODO: Description."""
        built = query.build()
        with self.cursor(connection) as cursor:
            _logger.debug("%s", built)
            cursor.execute(*built)
            yield cursor

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
        with self.execute(query) as cursor:
            pass
        return {"id": id}

    def read(self, id):
        where = self.query()
        where.text(f"{self.table.pk} = ")
        where.param(self.table.encode(self.table.pk, id))
        results = self.select(where=where)
        if len(results) == 0:
            raise NotFound()
        elif len(results) > 1:
            raise InternalServerError("query matches more than one row")
        result = results[0]
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
            value = self.table.encode(column, _body.get(column))
            query.param(value)
            if n < len(columns) - 1:
                query.text(", ")
        query.text(f" WHERE {self.table.pk} = ")
        query.param(self.table.encode(self.table.pk, id))
        query.text(";")
        with self.execute(query) as cursor:
            count = cursor.rowcount
        if count == 0:
            raise NotFound()
        elif count > 1:
            raise InternalServerError("query matches more than one row")
        elif count == -1:
            raise InternalServerError("could not determine update was successful")

    def delete(self, id):
        query = self.query()
        query.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
        query.param(self.table.encode(self.table.pk, id))
        query.text(";")
        with self.execute(query) as cursor:
            count = cursor.rowcount
        if count < 1:
            raise NotFound()
        elif count > 1:
            raise InternalServerError("query would delete more than one row")

    def list(self, where=None):
        """
        Return a list of primary keys that match the `where` expression.

        :param where: `Query` object representing WHERE expression, or `None` to match all rows.
        """
        query = self.query()
        query.text(f"SELECT {self.table.pk} FROM {self.table.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        with self.execute(query) as cursor:
            items = cursor.fetchall()
        return [self.table.decode(self.table.pk, item[0]) for item in items]

    def select(self, *, columns=None, where=None, order=None):
        """
        Return a list of rows that match the `where` expression. Each row is expressed in a dict.

        :param columns: iterable of column names to return, or `None` for all columns.
        :param where: `Query` object representing WHERE expression, or `None` to match all rows.
        :param order: iterable of column names to order by, or `None` to not order results.
        """
        columns = list(columns or self.table.columns)
        query = self.query()
        query.text("SELECT ")
        query.text(", ".join(columns))
        query.text(f" FROM {self.table.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        if order:
            query.text(f" ORDER BY {', '.join(order)}")
        query.text(";")
        results = []
        with self.execute(query) as cursor:
            items = cursor.fetchall()
        for item in items:
            result = dict(zip(columns, item))
            for column in columns:
                if result[column] is None and column not in self.table.schema.required:
                    del result[column]
                else:
                    result[column] = self.table.decode(column, result[column])
                    self.table.schema.properties[column].validate(result[column])
            results.append(result)
        return results
