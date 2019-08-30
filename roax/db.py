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


class Codec:
    def encode(self, schema, value):
        """Encode a value as a query parameter."""
        return schema.str_encode(value)

    def decode(self, schema, value):
        """Decode a value from a query result."""
        return schema.str_decode(value)


default_codec = Codec()


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

    def query(self, table):
        """Return a new query builder."""
        return Query(self, table)


class Table:
    """TODO: Description."""

    def __init__(self, name, schema, pk, codecs):
        """
        :param name: Name of table in the SQL database.
        :param schema: Schema of table columns.
        :param pk: Column name of the primary key.
        :param codecs: Column transformation adapters.
        """
        if not isinstance(schema, s.dict):
            raise ValueError("schema for table must be roax.schema.dict")
        super().__init__()
        self.name = name
        self.schema = schema
        if pk not in schema:
            raise ValueError(f"pk '{pk}' not in schema")
        self.pk = pk
        self.codecs = codecs

    @property
    def columns(self):
        return list(self.schema.properties.keys())

    def query(self, table):
        """Return a new query builder."""
        return Query(self, table)

    def codec(self, column):
        """Return a codec for the specified column."""
        schema = self.schema.properties[column]
        if column in self.codecs:
            return self.codecs[column]
        elif schema.__class__ in self.codecs:
            return self.codecs[schema.__class__]
        else:
            return default_codec


class Query:
    """
    Builds queries that manages its text and parameters.
    """

    def __init__(self, database, table):
        """
        :param database: database for which query is being built.
        :param table: table for which query is being built.
        """
        self.database = database
        self.table = table
        self.operation = []
        self.parameters = []

    def text(self, text):
        """Append text to the query operation."""
        self.operation.append(text)

    def param(self, param):
        """
        Append a parameter value to the query. A parameter marker is added to
        the query operation. No encoding of the parameter value is performed.
        """
        self.operation.append(
            _markers[self.database.paramstyle].format(len(self.parameters))
        )
        self.parameters.append(param)

    def params(self, params, sep=", "):
        """
        Append a set of parameter values to the query. Parameter markers are
        added to the query operation, separated by `sep`. No encoding of
        parameter values is performed.
        """
        for n in range(0, len(params)):
            self.param(params[n])
            if n < len(params) - 1:
                self.text(sep)

    def value(self, column, value):
        """
        Encode and add a column value to the query as a parameter.
        """
        schema = self.table.schema.properties[column]
        codec = self.table.codec(column)
        self.param(None if value is None else codec.encode(schema, value))

    def query(self, query):
        """
        Add another query's operation and parameters to the query.
        """
        self.operation += query.operation
        self.parameters += query.parameters

    def build(self):
        """
        TODO: Description.
        """
        return (
            "".join(self.operation),
            {str(n): self.parameters[n] for n in range(0, len(self.parameters))}
            if self.database.paramstyle in {"named", "pyformat"}
            else self.parameters,
        )


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
        """Return a new query builder for the resource."""
        return Query(self.database, self.table)

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
                None
                if _body.get(column) is None
                else self.table.codec(column).encode(
                    self.table.schema[column], _body.get(column)
                )
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
        where.value(self.table.pk, id)
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
            query.value(column, _body.get(column))
            if n < len(columns) - 1:
                query.text(", ")
        query.text(f" WHERE {self.table.pk} = ")
        query.value(self.table.pk, id)
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
        query.value(self.table.pk, id)
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
        pk = self.table.schema.properties[self.table.pk]
        return [self.table.codec(self.table.pk).decode(pk, item[0]) for item in items]

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
                    schema = self.table.schema.properties[column]
                    value = self.table.codec(column).decode(schema, result[column])
                    schema.validate(value)
                    result[column] = value
            results.append(result)
        return results
