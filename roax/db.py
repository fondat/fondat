"""
Module to manage resource items in a SQL database through DB-API 2.0 interface.
"""

import contextlib
import inspect
import logging
import roax.resource
import roax.schema as s

from dataclasses import dataclass


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
    implement the "connect" method.

    Parameters:
    • module: DB-API 2.0 module providing access to the database.
    • adapters: Iterable of adapters to be used with the database.
    """

    def __init__(self, module, adapters):
        self.module = module
        self._adapters = adapters.copy()

    def adapter(self, schema):
        """
        Return an adapter for the specified schema type.
        """
        cls = schema.__class__
        while True:
            try:
                return self._adapters[cls]
            except KeyError:
                try:
                    cls = cls.__bases__[0]  # naïve
                except IndexError:
                    break
        raise ValueError(f"no adapter for schema type {schema.__class__.__name__}")

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
        • connection: Connection to open cursor on.  [None]

        If no connection is supplied to allocate the cursor from, a connection
        is automatically fetched.
        """
        with _nullcontext(connection) if connection else self.connect() as connection:
            cursor = connection.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def query(self):
        """Return a new query builder for the database."""
        return Query(self)

    def create_table(self, table):
        """
        Create table in database.

        Parameters:
        • table: TODO.
        """
        query = self.query()
        query.text(f"CREATE TABLE {table.name} (")
        columns = []
        for name, schema in table.columns.items():
            column = f"{name} {self.adapter(schema).sql_type}"
            if name == table.pk:
                column += " PRIMARY KEY"
            if not schema.nullable:
                column += " NOT NULL"
            columns.append(column)
        query.text(", ".join(columns))
        query.text(");")
        query.execute()

    def drop_table(self, table):
        """
        Drop table in database.

        Parameters:
        • table: TODO.
        """
        query = self.query()
        query.text(f"DROP TABLE {table.name};")
        query.execute()

    def create_index(self, table, index):
        """
        Create index in database.

        Parameters:
        • table: TODO.
        • index: TODO.
        """
        query = self.query()
        query.text("CREATE ")
        if index.unique:
            query.text("UNIQUE ")
        query.text(f"INDEX {index.name} on {table.name} (")
        query.text(", ".join(index.columns))
        query.text(");")
        query.execute()

    def drop_index(self, index):
        """
        Drop index in database.

        Parameters:
        • index: TODO.
        """
        query = self.query()
        query.text(f"DROP INDEX {index.name};")
        query.execute()


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: Name of database table.
    • schema: Schema of table columns.
    • pk: Column name of the primary key.

    Attributes:
    • columns: Mapping of column names to associated schema types.

    The schema must be a roax.schema.dataclass type, whose attribute names map
    to table columns.
    """

    def __init__(self, name, schema, pk):
        self.name = name
        if not isinstance(schema, s.dataclass):
            raise ValueError("schema for table must be roax.schema.dataclass")
        self.schema = schema
        self.columns = self.schema.attrs.__dict__
        if pk not in self.columns:
            raise ValueError(f"primary key '{pk}' not in schema")
        self.pk = pk


@dataclass
class Index:
    """Represents a database index."""

    name: s.str()
    columns: s.list(s.str())
    unique: s.bool() = False


class Query:
    """
    Builds a database query.

    Parameter:
    • database: TODO.
    """

    def __init__(self, database):
        self.database = database
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

    def value(self, schema, value):
        """
        Encode and add a value to the query as a parameter.
        """
        try:
            self.param(
                None
                if value is None
                else self.database.adapter(schema).sql_encode(schema, value)
            )
        except s.SchemaError as se:
            se.path = column
            raise

    def query(self, query):
        """
        Add another query's operation and parameters to this query.
        """
        self.operation += query.operation
        self.parameters += query.parameters

    def queries(self, queries, separator=None):
        """
        Add a series of queries to this query, separated by an optional separator.

        Parametes:
        • separator: Separator between queries being added.
        • queries: Sequence of queries to be added to the query.
        """
        sep = False
        for query in queries:
            if sep and separator is not None:
                self.text(separator)
            self.query(query)
            sep = True

    def build(self):
        """
        Returns a tuple of (operator, parameters) suitable to supply as
        arguments to the cursor.execute method.
        """
        return (
            "".join(self.operation),
            {str(n): self.parameters[n] for n in range(0, len(self.parameters))}
            if self.database.paramstyle in {"named", "pyformat"}
            else self.parameters,
        )

    def execute(self, cursor=None):
        """
        Build and exeute the query.

        Parameter:
        • cursor: Cursor to execute query.
        """
        built = self.build()
        _logger.debug("%s", built)
        with _nullcontext(cursor) if cursor else self.database.cursor() as cursor:
            cursor.execute(*built)


class Adapter:
    """
    Adapts Roax schema type to database schema type.

    Each SQL database expects different Python representations for data types
    it supports. An adapter converts a value to and from a representation that
    is expected by the SQL database.

    Parameters and attributes:
    • schema: Schema of values to be encoded/decoded.
    • sql_type: The SQL type associated with the adapter.
   """

    def __init__(self, sql_type):
        self.sql_type = sql_type

    def sql_encode(self, schema, value):
        """
        Encode a value as a query parameter.
        """
        raise NotImplementedError

    def sql_decode(self, schema, value):
        """
        Decode a value from a query result.
        """
        raise NotImplementedError


class TableResource(roax.resource.Resource):
    """
    Base resource class for storage of resource items in a database table,
    providing basic CRUD operations.

    Parameters and attributes:
    • table Table that resource is based on.
    • database: Database the resource is attached to.
    • name: Short name of the resource.  [table.name in lower case]
    • description: Short description of the resource.  [resource docstring]

    This class does not decorate operations for schema and validation;
    subclasses are expected to do this based on their own requirements.
    """

    def __init__(self, table, *, database=None, name=None, description=None):
        self.table = table
        super().__init__(name or self.table.name.lower(), description)
        self.__doc__ = self.table.schema.description
        self.database = database

    def NotFound(self, id):
        return roax.resource.NotFound(
            f"{self.name} item not found: {self.table.columns[self.table.pk].str_encode(id)}"
        )

    def create(self, id, _body):
        self.table.schema.validate(_body)
        query = self.database.query()
        query.text(f"INSERT INTO {self.table.name} (")
        query.text(", ".join(self.table.columns))
        query.text(") VALUES (")
        columns = self.table.columns
        comma = False
        for column, schema in columns.items():
            if comma:
                query.text(", ")
            query.value(schema, getattr(_body, column))
            comma = True
        query.text(");")
        query.execute()
        return {"id": id}

    def read(self, id):
        database = self.database
        where = database.query()
        where.text(f"{self.table.pk} = ")
        where.value(self.table.columns[self.table.pk], id)
        results = self.select(where=where)
        if len(results) == 0:
            raise self.NotFound(id)
        elif len(results) > 1:
            raise roax.resource.InternalServerError("query matches more than one row")
        kwargs = results[0]
        try:
            result = self.table.schema.cls(**kwargs)
            self.table.schema.validate(result)
        except (TypeError, s.SchemaError) as e:
            _logger.error(e)
            raise roax.resource.InternalServerError
        return result

    def _update(self, id, *, old=None, new):
        self.table.schema.validate(new)
        database = self.database
        query = database.query()
        query.text(f"UPDATE {self.table.name} SET ")
        columns = self.table.columns
        updates = []
        for column, schema in columns.items():
            if old is None or getattr(old, column, None) != getattr(new, column, None):
                update = self.database.query()
                update.text(f"{column} = ")
                update.value(self.table.columns[column], getattr(new, column, None))
                updates.append(update)
        query.queries(updates, ", ")
        query.text(f" WHERE {self.table.pk} = ")
        query.value(self.table.columns[self.table.pk], id)
        query.text(";")
        with database.cursor() as cursor:
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

    def update(self, id, _body):
        self._update(id, new=_body)

    def patch(self, id, _body):
        for column, schema in self.table.columns.items():
            if isinstance(schema, s.bytes) and schema.format == "binary":
                raise ResourceError(
                    "patch not supported on dataclass with bytes attribute of binary format"
                )
        schema = self.table.schema
        old = self.read(id)
        new = schema.json_decode(roax.patch.merge_patch(schema.json_encode(old), _body))
        if old != new:
            self._update(id, old=old, new=new)

    def delete(self, id):
        database = self.database
        query = database.query()
        query.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
        query.value(self.table.columns[self.table.pk], id)
        query.text(";")
        with database.cursor() as cursor:
            query.execute(cursor)
            count = cursor.rowcount
        if count < 1:
            raise self.NotFound(id)
        elif count > 1:
            raise roax.resource.InternalServerError(
                "query would delete more than one row"
            )

    def select(self, *, columns=None, where=None, order=None):
        """
        Return a list of rows that match the WHERE expression. Each row is expressed in a dict.

        Parameters:
        • columns: Iterable of column names to return, or None for all columns.
        • where: Query object representing WHERE expression, or None to match all rows.
        • order: Iterable of column names to order by, or None to not order results.
        """
        database = self.database
        table = self.table
        columns = tuple(columns or table.columns.keys())
        query = database.query()
        query.text("SELECT ")
        query.text(", ".join(columns))
        query.text(f" FROM {table.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        if order:
            query.text(f" ORDER BY ")
            query.text(", ".join(order))
        query.text(";")
        results = []
        with database.cursor() as cursor:
            query.execute(cursor)
            items = cursor.fetchall()
        for item in items:
            result = dict(zip(columns, item))
            for column in columns:
                schema = table.columns[column]
                if result[column] is not None:
                    try:
                        result[column] = database.adapter(schema).sql_decode(
                            schema, result[column]
                        )
                    except s.SchemaError as se:
                        se.path = column
                        raise
            results.append(result)
        return results

    def list(self, *, where=None):
        """
        Return a list of primary keys that match the where expression.

        Parameters:
        • where: Query object representing WHERE expression, or None to match all rows.
        """
        database = self.database
        table = self.table
        query = database.query()
        query.text(f"SELECT {table.pk} FROM {table.name}")
        if where:
            query.text(" WHERE ")
            query.query(where)
        with database.cursor() as cursor:
            query.execute(cursor)
            items = cursor.fetchall()
        schema = table.columns[table.pk]
        return [database.adapter(schema).sql_decode(schema, item[0]) for item in items]
