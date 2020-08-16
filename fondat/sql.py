"""
Module to manage resource items in a SQL database through DB-API 2.0 interface.
"""

import collections.abc
import contextlib
import inspect
import logging
import roax.patch
import roax.resource
import roax.schema as s

from dataclasses import dataclass


_logger = logging.getLogger(__name__)


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


class Adapters(collections.abc.Mapping):
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


class Database:
    """
    Base class for a SQL database. Subclasses must implement the
    "transaction" method and expose the "marker".

    Parameters:
    • adapters: Mapping of adapters to be used with the database.
    """

    def __init__(self, adapters):
        self.adapters = Adapters(adapters)

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

    async def create_table(self, table):
        """
        Create table in database.

        Parameters:
        • table: Object representing table to create.
        """
        stmt = Statement()
        stmt.text(f"CREATE TABLE {table.name} (")
        columns = []
        for name, schema in table.columns.items():
            column = [name, self.adapters[schema].sql_type]
            if name == table.pk:
                column.append("PRIMARY KEY")
            if not schema.nullable:
                column.append("NOT NULL")
            columns.append(" ".join(column))
        stmt.text(", ".join(columns))
        stmt.text(");")
        async with self.transaction() as t:
            await t.execute(stmt)

    async def drop_table(self, table):
        """
        Drop table in database.

        Parameters:
        • table: Object representing table to drop.
        """
        stmt = Statement()
        stmt.text(f"DROP TABLE {table.name};")
        async with self.transaction() as t:
            await t.execute(stmt)

    async def create_index(self, index):
        """
        Create index in database.

        Parameters:
        • index: Object representing index to create.
        """
        stmt = Statement()
        stmt.text("CREATE ")
        if index.unique:
            stmt.text("UNIQUE ")
        stmt.text(f"INDEX {index.name} on {index.table.name} (")
        stmt.text(", ".join(index.columns))
        stmt.text(");")
        async with self.transaction() as t:
            await t.execute(stmt)

    async def drop_index(self, index):
        """
        Drop index in database.

        Parameters:
        • index: Object representing index to drop.
        """
        stmt = Statement()
        stmt.text(f"DROP INDEX {index.name};")
        async with self.transaction() as t:
            await t.execute(stmt)


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


class Statement:
    """
    Builds a SQL statement. 
    """

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

    def statement(self, statement):
        """
        Add another statement to this statement.
        """
        self.operation += statement.operation
        self.parameters += statement.parameters

    def statements(self, statements, separator=None):
        """
        Add a sequence of statements to this statement, optionally separating
        each with optional separator.

        Parametes:
        • statements: Sequence of statements to be added to the statement.
        • separator: Separator between statements to be added.
        """
        sep = False
        for statement in statements:
            if sep and separator is not None:
                self.text(separator)
            self.statement(statement)
            sep = True


class TableResource:
    """
    Base resource class for storage of resource items in a database table,
    providing basic CRUD operations.

    Parameters and attributes:
    • table Table that resource is based on.
    • database: Database the resource is attached to.

    This class does not decorate operations for schema and validation;
    subclasses are expected define decorated operation methods as required.
    """

    def __init__(self, table, database):
        super().__init__()
        self.table = table
        self.database = database

    async def create(self, id, _body):
        self.table.schema.validate(_body)
        stmt = Statement()
        stmt.text(f"INSERT INTO {self.table.name} (")
        stmt.text(", ".join(self.table.columns))
        stmt.text(") VALUES (")
        columns = self.table.columns
        comma = False
        for column, schema in columns.items():
            if comma:
                stmt.text(", ")
            stmt.param(getattr(_body, column), schema)
            comma = True
        stmt.text(");")
        async with self.database.transaction() as t:
            await t.execute(stmt)
        return {"id": id}

    async def read(self, id):
        where = Statement()
        where.text(f"{self.table.pk} = ")
        where.param(id, self.table.columns[self.table.pk])
        results = await self.select(where=where)
        if len(results) == 0:
            raise roax.resource.NotFound(
                f"item not found: {self.table.columns[self.table.pk].str_encode(id)}"
            )
        elif len(results) > 1:
            raise roax.resource.InternalServerError("result matches more than one row")
        kwargs = results[0]
        try:
            result = self.table.schema.class_(**kwargs)
            self.table.schema.validate(result)
        except (TypeError, s.SchemaError) as e:
            _logger.error(e)
            raise roax.resource.InternalServerError
        return result

    async def _update(self, id, *, old=None, new):
        self.table.schema.validate(new)
        stmt = Statement()
        stmt.text(f"UPDATE {self.table.name} SET ")
        columns = self.table.columns
        updates = []
        for column, schema in columns.items():
            if old is None or getattr(old, column, None) != getattr(new, column, None):
                update = Statement()
                update.text(f"{column} = ")
                update.param(getattr(new, column, None), self.table.columns[column])
                updates.append(update)
        stmt.statements(updates, ", ")
        stmt.text(f" WHERE {self.table.pk} = ")
        stmt.param(id, self.table.columns[self.table.pk])
        stmt.text(";")
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def update(self, id, _body):
        async with self.database.transaction():  # transaction demaraction
            await self.read(id)  # raises NotFound
            await self._update(id, new=_body)

    async def patch(self, id, _body):
        for column, schema in self.table.columns.items():
            if isinstance(schema, s.bytes) and schema.format == "binary":
                raise ResourceError(
                    "patch not supported on dataclass with bytes attribute of binary format"
                )
        async with self.database.transaction():  # transaction demaraction
            old = await self.read(id)
            new = self.table.schema.json_decode(
                roax.patch.merge_patch(self.table.schema.json_encode(old), _body)
            )
            if old != new:
                await self._update(id, old=old, new=new)

    async def delete(self, id):
        stmt = Statement()
        stmt.text(f"DELETE FROM {self.table.name} WHERE {self.table.pk} = ")
        stmt.param(id, self.table.columns[self.table.pk])
        stmt.text(";")
        async with self.database.transaction() as t:
            await self.read(id)  # raises NotFound
            await t.execute(stmt)

    async def select(self, columns=None, where=None, order=None):
        """
        Return a list of rows that match the WHERE expression. Each row is expressed in a dict.

        Parameters:
        • columns: Iterable of column names to return, or None for all columns.
        • where: Statement containing WHERE expression, or None to match all rows.
        • order: Iterable of column names to order by, or None to not order results.
        """
        table = self.table
        columns = tuple(columns or table.columns.keys())
        stmt = Statement()
        stmt.text("SELECT ")
        stmt.text(", ".join(columns))
        stmt.text(f" FROM {table.name}")
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
                    schema = table.columns[column]
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

        return [
            row[self.table.pk] for row in await self.select((self.table.pk,), where)
        ]
