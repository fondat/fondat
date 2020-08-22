"""
Module to manage resource items in a SQL database.
"""

import collections.abc
import contextlib
import fondat.patch
import fondat.resource
import fondat.schema as s
import inspect
import logging

from dataclasses import dataclass


_logger = logging.getLogger(__name__)


class Adapter:
    """
    Adapts a Fondat schema type to a database schema type.

    Each SQL database expects different Python representations for data types
    it supports. An adapter converts a value to and from a representation that
    is expected by the SQL database.

    Parameters and attributes:
    • schema: Schema of values to be encoded/decoded.
    • sql_type: The SQL type associated with the adapter.
   """

    def __init__(self, sql_type):
        self.sql_type = sql_type

    def sql_encode(self, value, schema):
        """
        Encode a value as a statement parameter.
        """
        raise NotImplementedError

    def sql_decode(self, value, schema):
        """
        Decode a value from a query result.
        """
        raise NotImplementedError


class _Adapters(collections.abc.Mapping):
    """
    A mapping of schema classes to associated adapters.

    Parameter:
    • adapters: Mapping of adatpers to initialize.
    """

    def __init__(self, adapters=None):
        self.adapters = {k: v for k, v in adapters.items()} if adapters else {}

    def __getitem__(self, key):
        """Return an adapter for the specified schema type."""
        cls = key.__class__
        while True:
            try:
                return self.adapters[cls]
            except KeyError:
                try:
                    cls = cls.__bases__[0]  # naïve
                except IndexError:
                    break
        raise KeyError(f"no adapter for schema type {schema.__class__.__name__}")

    def __iter__(self):
        return iter(self.adapters)

    def __len__(self):
        return len(self.adapters)


class Statement:
    """Builds a SQL statement."""

    PARAM = object()

    def __init__(self):
        self.operation = []
        self.parameters = []

    def text(self, text):
        """Append text to the statement."""
        self.operation.append(text)

    def param(self, value, schema=None):
        """
        Append a parameter value to the statement.
        
        If a schema is provided, the value will be encoded to SQL when the
        statement is executed.

        Parametes:
        • value: The value of parameter to append.
        • schema: Schema of the parameter value to append.
        """
        self.operation.append(Statement.PARAM)
        self.parameters.append((value, schema))

    def params(self, params, separator=None):
        """
        Append a sequence of parameters to this statement, optionally
        separating each with a separator.

        Parameters:
        • params: Sequence of tuples (value, schema) to be added.
        • separator: Text separator between parameters.
        """
        sep = False
        for param in params:
            if sep and separator is not None:
                self.text(separator)
            self.param(*param)
            sep = True

    def statement(self, statement):
        """
        Add another statement to this statement.
        """
        self.operation += statement.operation
        self.parameters += statement.parameters

    def statements(self, statements, separator=None):
        """
        Add a sequence of statements to this statement, optionally separating
        each with a separator.

        Parameters:
        • statements: Sequence of statements to be added to the statement.
        • separator: Separator between statements to be added.
        """
        sep = False
        for statement in statements:
            if sep and separator is not None:
                self.text(separator)
            self.statement(statement)
            sep = True


class Database:
    """
    Base class for a SQL database. Subclasses must implement the
    "transaction" method and expose the "marker".

    Parameters:
    • adapters: Mapping of adapters to be used with the database.
    """

    def __init__(self, adapters):
        self.adapters = _Adapters(adapters)

    async def transaction(self):
        """
        Return a context manager that manages a database transaction.

        A transaction provides the means to execute a SQL statement, and
        provides transaction semantics (commit/rollback). Upon exit of the
        context manager, in the event of an exception, the transaction will be
        rolled back; otherwise, the transaction will be committed. 

        If more than one request for a transaction is made within the same
        task, the same transaction will be returned; only the outermost
        yielded transaction will exhibit commit/rollback behavior.
        """
        raise NotImplementedError


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: Name of database table.
    • schema: Schema of table columns.
    • pk: Column name of the primary key.

    Attributes:
    • columns: Mapping of column names to associated schema types.

    The schema must be a fondat.schema.dataclass type, whose attribute names map
    to table columns.
    """

    def __init__(self, database, name, schema, pk):
        if not isinstance(schema, s.dataclass):
            raise ValueError("schema for table must be fondat.schema.dataclass")
        self.database = database
        self.name = name
        self.schema = schema
        self.columns = self.schema.attrs.__dict__
        if pk not in self.columns:
            raise ValueError(f"primary key '{pk}' not in schema")
        self.pk = pk

    async def create(self):
        """Create table in database."""
        stmt = Statement()
        stmt.text(f"CREATE TABLE {self.name} (")
        columns = []
        for name, schema in self.columns.items():
            column = [name, self.database.adapters[schema].sql_type]
            if name == self.pk:
                column.append("PRIMARY KEY")
            if not schema.nullable:
                column.append("NOT NULL")
            columns.append(" ".join(column))
        stmt.text(", ".join(columns))
        stmt.text(");")
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def drop(self):
        """Drop table from database."""
        stmt = Statement()
        stmt.text(f"DROP TABLE {self.name};")
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def select(self, columns=None, where=None, order=None):
        """
        Return a list of rows in table that match the where expression. Each
        row is expressed in a dict.

        Parameters:
        • columns: Column names to return, or None for all columns.
        • where: Statement containing WHERE expression, or None to match all rows.
        • order: Column names to order by, or None to not order results.
        """
        columns = tuple(columns or self.columns.keys())
        stmt = Statement()
        stmt.text("SELECT ")
        stmt.text(", ".join(columns))
        stmt.text(f" FROM {self.name}")
        if where:
            stmt.text(" WHERE ")
            stmt.statement(where)
        if order:
            stmt.text(f" ORDER BY ")
            stmt.text(", ".join(order))
        stmt.text(";")
        results = []
        async with self.database.transaction() as t:
            async for row in await t.execute(stmt):
                result = dict(zip(columns, row))
                for column in columns:
                    schema = self.columns[column]
                    if result[column] is not None:
                        try:
                            result[column] = self.database.adapters[schema].sql_decode(
                                result[column], schema
                            )
                        except s.SchemaError as se:
                            se.path = column
                            raise
                results.append(result)
        return results

    async def list(self, where=None):
        """
        Return a list of primary keys that match the where expression.

        Parameters:
        • where: Statement containing WHERE expression, or None to list all rows.
        """
        stmt = Statement()
        stmt.text(f"SELECT {self.pk} FROM {self.name}")
        if where:
            stmt.text(" WHERE ")
            stmt.statement(where)
        stmt.text(";")
        pk_schema = self.columns[self.pk]
        result = []
        async with self.database.transaction() as t:
            async for row in await t.execute(stmt):
                result.append(
                    self.database.adapters[pk_schema].sql_decode(row[0], pk_schema)
                )
        return result


class Index:
    """
    Represents an index on a table in a SQL database.

    Parameters:
    • name: the name of the index.
    • table: table that the index defined for.
    • columns: list of column names to index.
    • unique: are columns unique in table.
    """

    def __init__(self, name, table, columns, unique=False):
        self.name = name
        self.table = table
        for column in columns:
            if column not in table.columns:
                raise ValueError(f"column '{column}' not in {table.name} table")
        self.columns = columns
        self.unique = unique

    async def create(self):
        """Create index in database."""
        stmt = Statement()
        stmt.text("CREATE ")
        if index.unique:
            stmt.text("UNIQUE ")
        stmt.text(f"INDEX {self.name} on {self.table.name} (")
        stmt.text(", ".join(self.columns))
        stmt.text(");")
        async with self.table.database.transaction() as t:
            await t.execute(stmt)

    async def drop(self):
        """Drop index from database."""
        stmt = Statement()
        stmt.text(f"DROP INDEX {self.name};")
        async with self.transaction() as t:
            await t.execute(stmt)


def table_resource(table, security=None):
    """Return a new table resource class."""

    @fondat.resource.resource
    class TableResource:
        def __init__(self):
            self.table = table

        @fondat.resource.operation(security=security)
        async def get(self, id: table.columns[table.pk]) -> table.schema:
            where = Statement()
            where.text(f"{self.table.pk} = ")
            where.param(id, self.table.columns[table.pk])
            results = await self.table.select(where=where)
            try:
                kwargs = next(iter(results))
            except StopIteration:
                raise fondat.resource.NotFound(
                    f"row not found: {self.table.columns[self.table.pk].str_encode(id)}"
                )
            try:
                result = self.table.schema.class_(**kwargs)
                self.table.schema.validate(
                    result
                )  # TODO: simplify and just check for required?
            except (TypeError, s.SchemaError) as e:
                _logger.error(e)
                raise fondat.resource.InternalServerError
            return result

        async def _update(self, id, old, new):
            if old == new:
                return
            self.table.schema.validate(new)
            stmt = Statement()
            stmt.text(f"UPDATE {self.table.name} SET ")
            columns = self.table.columns
            updates = []
            for column, schema in columns.items():
                if getattr(old, column, None) != getattr(new, column, None):
                    update = Statement()
                    update.text(f"{column} = ")
                    update.param(getattr(new, column, None), self.table.columns[column])
                    updates.append(update)
            stmt.statements(updates, ", ")
            stmt.text(f" WHERE {self.table.pk} = ")
            stmt.param(id, self.table.columns[self.table.pk])
            stmt.text(";")
            async with self.table.database.transaction() as t:
                await t.execute(stmt)

        @fondat.resource.operation(security=security)
        async def put(self, id: table.columns[table.pk], data: table.schema):
            await self._update(id, await self.get(id), data)

        @fondat.resource.operation(security=security)
        async def post(
            self, id: table.columns[table.pk], data: table.schema
        ) -> s.dict({"id": table.columns[table.pk]}):
            setattr(data, self.table.pk, id)
            self.table.schema.validate(data)
            stmt = Statement()
            stmt.text(f"INSERT INTO {self.table.name} (")
            stmt.text(", ".join(self.table.columns))
            stmt.text(") VALUES (")
            stmt.params(
                (
                    (getattr(data, column), schema)
                    for column, schema in self.table.columns.items()
                ),
                ", ",
            )
            stmt.text(");")
            async with self.table.database.transaction() as t:
                await t.execute(stmt)
            return dict(id=id)

        @fondat.resource.operation(security=security)
        async def delete(self, id: table.columns[table.pk]):
            await self.get(id)
            stmt = Statement()
            stmt.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
            stmt.param(id, self.table.columns[self.table.pk])
            stmt.text(";")
            async with self.table.database.transaction() as t:
                await t.execute(stmt)

        @fondat.resource.operation(security=security)
        async def patch(
            self, id: table.columns[table.pk], doc: s.dict({}, additional=True)
        ):
            old = await self.get(id)
            new = self.table.schema.json_decode(
                fondat.patch.merge_patch(self.table.schema.json_encode(old), doc)
            )
            await self._update(id, old, new)

    return TableResource
