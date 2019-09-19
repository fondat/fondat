"""
Module to manage resource items in a SQL database through DB-API 2.0 interface.
"""

import contextlib
import logging
import roax.resource
import roax.schema


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


class Database:
    """
    Base class to manage connections to a SQL database. Subclasses must
    implement the "connect" method, and expose an "adapters" attribute.

    Parameter:
    • module: DB-API 2.0 module providing access to the database.
    """

    def __init__(self, module):
        self.module = module

    @property
    def paramstyle(self):
        return self.module.paramstyle

    def connect(self):
        """
        Return a context manager that yields a database connection, providing
        transaction demarcation (commit/rollback on exit). Upon exit of the
        context manager, in the event of an exception, the transaction is
        rolled back; otherwise, the transaction is committed. 

        If more than one request for a connection is made in the same thread,
        the same connection will be returned; only the outermost yielded
        connection shall exhibit transaction demarcation.
        """
        raise NotImplementedError

    @contextlib.contextmanager
    def cursor(self, connection=None):
        """
        Return a context manager that yields a database cursor that automatically
        closes on exit.

        Parameter:
        • connection Connection to open cursor on.  [None]

        If no connection is supplied to allocate the cursor from, a connection
        is automatically fetched.
        """
        with _nullcontext(connection) if connection else self.connect() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()


class Table:
    """
    Represents a table in a SQL database.

    Parameters and instance variables:
    • database: Database where table resides.
    • name: Name of database table.
    • schema: Schema of table columns.
    • pk: Column name of the primary key.
    • adapters: Column transformation adapters.

    Instance variables:
    • columns: Mapping of column name to associated schema type.

    The schema must be a roax.schema.dataclass type, whose attribute names map
    to table columns.

    The adapters parameter is a dict of keys-to-adapter instances. A key can be
    either a column name or a roax.schema type.
    """

    def __init__(self, database, name, schema, pk, adapters=None):
        super().__init__()
        self.database = database
        self.name = name
        if not isinstance(schema, roax.schema.dataclass):
            raise ValueError("schema for table must be roax.schema.dataclass")
        self.schema = schema
        self.columns = self.schema.attrs.__dict__
        if pk not in self.columns:
            raise ValueError(f"primary key '{pk}' not in schema")
        self.pk = pk
        self.adapters = {**database.adapters, **(adapters if adapters else {})}

    def adapter(self, column):
        """
        Return an adapter for the specified column. An adapter is selected,
        matching first based on column name, on the schema type of the
        column, then defaulting to a string encoding/decoding adapter.

        Parameter:
        • column: Name of column to return adapter for.
        """
        return (
            self.adapters.get(column)
            or self.adapters.get(self.columns[column].__class__)
            or default_adapter
        )

    def connect(self):
        """
        Return a context manager that yields a database connection, providing
        transaction demarcation (commit/rollback on exit).
        """
        return self.database.connect()

    def cursor(self, connection=None):
        """
        Return a context manager that yields a database cursor that automatically
        closes on exit.

        Parameter:
        • connection Connection to open cursor on.  [None]
        """
        return self.database.cursor(connection)

    def query(self):
        """Return a new query builder for the table."""
        return Query(self)

    def select(self, *, columns=None, where=None, order=None):
        """
        Return a list of rows that match the WHERE expression. Each row is expressed in a dict.

        Parameters:
        • columns: Iterable of column names to return, or None for all columns.
        • where: Query object representing WHERE expression, or None to match all rows.
        • order: Iterable of column names to order by, or None to not order results.
        """
        columns = tuple(columns or self.columns.keys())
        query = self.query()
        query.text("SELECT ")
        query.text(", ".join(columns))
        query.text(f" FROM {self.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        if order:
            query.text(f" ORDER BY {', '.join(order)}")
        query.text(";")
        results = []
        with self.cursor() as cursor:
            query.execute(cursor)
            items = cursor.fetchall()
        for item in items:
            result = dict(zip(columns, item))
            for column in columns:
                schema = self.columns[column]
                if result[column] is not None or column in self.schema.required:
                    value = self.adapter(column).decode(schema, result[column])
                    result[column] = value
            results.append(result)
        return results

    def list(self, where=None):
        """
        Return a list of primary keys that match the where expression.

        Parameter:
        • where: Query object representing WHERE expression, or None to match all rows.
        """
        query = self.query()
        query.text(f"SELECT {self.pk} FROM {self.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        with self.cursor() as cursor:
            query.execute(cursor)
            items = cursor.fetchall()
        pk = self.columns[self.pk]
        return [self.adapter(self.pk).decode(pk, item[0]) for item in items]


class Query:
    """
    Builds a database query for a table.

    Parameter and instance variable:
    • table: Table for which query is to be built.
    """

    def __init__(self, table):
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
            _markers[self.table.database.paramstyle].format(len(self.parameters))
        )
        self.parameters.append(param)

    def value(self, column, value):
        """
        Encode and add a column value to the query as a parameter.
        """
        schema = self.table.columns[column]
        adapter = self.table.adapter(column)
        self.param(None if value is None else adapter.encode(schema, value))

    def query(self, query):
        """
        Add another query's operation and parameters to the query.
        """
        self.operation += query.operation
        self.parameters += query.parameters

    def build(self):
        """
        Returns a tuple of (operator, parameters) suitable to supply as
        arguments to the cursor.execute method.
        """
        return (
            "".join(self.operation),
            {str(n): self.parameters[n] for n in range(0, len(self.parameters))}
            if self.table.database.paramstyle in {"named", "pyformat"}
            else self.parameters,
        )

    def execute(self, cursor):
        """
        Build and exeute the query.

        Parameter:
        • cursor: Cursor to execute query under.
        """
        built = self.build()
        _logger.debug("%s", built)
        cursor.execute(*built)


class Adapter:
    """
    Encodes values for queries and decodes values from query results.

    Each SQL database expects different Python representations for data types
    it supports. The purpose of an adapter is to convert a value to and from a
    representation that is expected by a SQL database. 

    This default implementation encodes to and from string value using the
    schema object's str_encode and str_decode methods.
    """

    def encode(self, schema, value):
        """
        Encode a value as a query parameter.

        Parameter:
        • schema: Schema of the value to be encoded in the query.
        • value: Value to be encoded.
        """
        return schema.str_encode(value)

    def decode(self, schema, value):
        """
        Decode a value from a query result.

        Parameters:
        • schema: Schema of the value to be decoded from the query result.
        • value: Value from the query result to be decoded.
        """
        return schema.str_decode(value)


default_adapter = Adapter()


class TableResource(roax.resource.Resource):
    """
    Base resource class for storage of resource items in a database table,
    providing basic CRUD operations.

    Parameters:
    • table Table that resource is based on.
    • name: Short name of the resource.  [table.name]
    • description: Short description of the resource.  [resource docstring]

    This class does not decorate operations or validate the schema of items;
    subclasses are expected to do this.
    """

    def __init__(self, table=None, name=None, description=None):
        self.table = table or self.table
        super().__init__(name or self.table.name, description)
        self.__doc__ = self.table.schema.description

    def NotFound(self, id):
        return roax.resource.NotFound(
            f"{self.name} item not found: {self.table.pk.str_encode(id)}"
        )

    def create(self, id, _body):
        query = self.table.query()
        query.text(f"INSERT INTO {self.table.name} (")
        query.text(", ".join(self.table.columns))
        query.text(") VALUES (")
        columns = self.table.columns
        comma = False
        for column, schema in columns.items():
            if comma:
                query.text(", ")
            query.value(column, getattr(_body, column))
            comma = True
        query.text(");")
        with self.table.cursor() as cursor:
            query.execute(cursor)
        return {"id": id}

    def read(self, id):
        where = self.table.query()
        where.text(f"{self.table.pk} = ")
        where.value(self.table.pk, id)
        results = self.table.select(where=where)
        if len(results) == 0:
            raise self.NotFound(id)
        elif len(results) > 1:
            raise roax.resource.InternalServerError("query matches more than one row")
        result = results[0]
        print(f"**RESULT={result}")
        return self.table.schema.cls(**result)

    def update(self, id, _body):
        query = self.table.query()
        query.text(f"UPDATE {self.table.name} SET ")
        columns = self.table.columns
        comma = False
        for column, schema in columns.items():
            if comma:
                query.text(", ")
            query.text(f"{column} = ")
            query.value(column, getattr(_body, column, None))
            comma = True
        query.text(f" WHERE {self.table.pk} = ")
        query.value(self.table.pk, id)
        query.text(";")
        with self.table.cursor() as cursor:
            query.execute(cursor)
            count = cursor.rowcount
        if count == 0:
            raise self.NotFound(id)
        elif count > 1:
            raise roax.resource.InternalServerError("query matches more than one row")
        elif count == -1:
            raise roax.resource.InternalServerError(
                "could not determine update was successful"
            )

    def delete(self, id):
        query = self.table.query()
        query.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
        query.value(self.table.pk, id)
        query.text(";")
        with self.table.cursor() as cursor:
            query.execute(cursor)
            count = cursor.rowcount
        if count < 1:
            raise self.NotFound(id)
        elif count > 1:
            raise roax.resource.InternalServerError(
                "query would delete more than one row"
            )
