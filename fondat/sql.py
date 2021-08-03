"""Module to manage data in a SQL database."""

from __future__ import annotations

import fondat.error
import fondat.patch
import fondat.security
import fondat.types
import functools
import logging
import typing
import wrapt

from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass, is_dataclass
from fondat.codec import get_codec, Binary, JSON
from fondat.data import datacls
from fondat.error import BadRequestError, NotFoundError
from fondat.memory import memory_resource
from fondat.patch import json_merge_patch
from fondat.resource import resource, operation, query
from fondat.validation import validate
from typing import Annotated, Any, Optional, TypedDict, Union


_logger = logging.getLogger(__name__)


def is_nullable(python_type):
    """Return if Python type allows for None value."""
    NoneType = type(None)
    if typing.get_origin(python_type) is Annotated:
        python_type = typing.get_args(python_type)[0]  # strip annotation
    if python_type is NoneType:
        return True
    if typing.get_origin(python_type) is not Union:
        return False
    for arg in typing.get_args(python_type):
        if arg is NoneType:
            return True
    return False


@dataclass
class Parameter:
    """
    Represents a parameterized value to include in a statement.

    Attributes:
    • value: the value of the parameter to be included
    • python_type: the type of the pameter to be included
    """

    value: Any
    python_type: Any


class Statement(Iterable):
    """
    Represents a SQL statement.

    Parameter and attribute:
    • result: the type to return a query result row in

    The result can be expressed as a dataclass to be instantiated, or as a TypedDict that
    results in a populated dict object.
    """

    slots = ("fragments", "result")

    def __init__(self, result=None):
        self.fragments = []
        self.result = result

    def __repr__(self):
        return f"Statement(fragments={self.fragments}, result={self.result})"

    def __iter__(self):
        """Iterate over fragments of the statement."""
        return iter(self.fragments)

    def __bool__(self):
        return len(self.fragments) > 0

    def text(self, value: str) -> None:
        """Append text to the statement."""
        self.fragments.append(value)

    def param(self, value: Any, python_type: Any = None) -> None:
        """
        Append a parameter to the statement.

        Parameters:
        • value: parameter value to be appended
        • python_type: parameter type; inferred from value if None
        """
        self.fragments.append(Parameter(value, python_type if python_type else type(value)))

    def parameter(self, parameter: Parameter) -> None:
        """Append a parameter to the statement."""
        self.fragments.append(parameter)

    def parameters(self, params: Iterable[Parameter], separator: str = None) -> None:
        """
        Append parameters to this statement, with optional text separator.

        Parameters:
        • params: parameters to be appended
        • separator: separator between parameters
        """
        sep = False
        for param in params:
            if sep and separator is not None:
                self.text(separator)
            self.parameter(param)
            sep = True

    def statement(self, statement: Statement) -> None:
        """Append a statement to this statement."""
        self.fragments += statement.fragments

    def statements(self, statements: Iterable[Statement], separator: str = None) -> None:
        """
        Append statements to this statement, with optional text separator.

        Parameters:
        • statements: statements to be added to the statement
        • separator: separator between statements
        """
        sep = False
        for statement in statements:
            if sep and separator is not None:
                self.text(separator)
            self.statement(statement)
            sep = True


class Database:
    """Base class for a SQL database."""

    async def connection(self):
        """
        Return a context manager that establishes a connection to the database. If a
        connection context has already been established, this method has no effect. Upon
        exit of the outermost connection context, the database connection is closed.
        """
        raise NotImplementedError

    async def transaction(self):
        """
        Return a context manager that represents a database transaction in which statements
        are executed.

        If a connection context is not established prior to the creation of the transaction
        context, a connection context is created and is used for the duration of the
        transaction.

        Upon exit of a transaction context, if an exception was raised, changes will be
        rolled back; otherwise changes will be committed.

        Transactions can be nested; committing the outermost transaction commits all changes
        to the database.
        """
        raise NotImplementedError

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[Any]]:
        """
        Execute a SQL statement.

        A database transaction must be established prior to the execution of a statement.

        If the statement is a query that expects results, then the type of each row to be
        returned is specified in the statement's "result" attribute; result rows are accessed
        via a returned asynchronus iterator.

        Parameter:
        • statement: statement to excute
        """
        raise NotImplementedError

    def get_codec(self, python_type: Any) -> Any:
        """
        Return a codec suitable for encoding/decoding a Python value to/from a
        corresponding SQL value.
        """
        raise NotImplementedError


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: name of database table
    • database: database where table is managed
    • schema: dataclass or TypedDict type representing the table schema
    • pk: column name of primary key

    Attributes:
    • columns: mapping of column names to ther associated types
    """

    __slots__ = ("name", "database", "schema", "columns", "pk")

    def __init__(self, name: str, database: Database, schema: type, pk: str):
        self.name = name
        self.database = database
        schema, _ = fondat.types.split_annotated(schema)
        if not is_dataclass(schema):
            raise TypeError("table schema must be a dataclass")
        self.schema = schema
        self.columns = typing.get_type_hints(schema, include_extras=True)
        if pk not in self.columns:
            raise ValueError(f"primary key not in schema: {pk}")
        self.pk = pk

    def __repr__(self):
        return f"Table(name={self.name}, schema={self.schema}, pk={self.pk})"

    async def create(self):
        """Create table in database."""
        stmt = Statement()
        stmt.text(f"CREATE TABLE {self.name} (")
        columns = []
        for column_name, column_type in self.columns.items():
            column = [column_name, self.database.get_codec(column_type).sql_type]
            if column_name == self.pk:
                column.append("PRIMARY KEY")
            if not is_nullable(column_type):
                column.append("NOT NULL")
            columns.append(" ".join(column))
        stmt.text(", ".join(columns))
        stmt.text(");")
        await self.database.execute(stmt)

    async def drop(self):
        """Drop table from database."""
        stmt = Statement()
        stmt.text(f"DROP TABLE {self.name};")
        await self.database.execute(stmt)

    async def select(
        self,
        *,
        columns: Union[Sequence[str], str] = None,
        where: Statement = None,
        order: Union[Sequence[str], str] = None,
        limit: int = None,
        offset: int = None,
    ) -> AsyncIterator[Any]:
        """
        Return an asynchronous iterable for rows in table that match the WHERE statement.
        Each row item is a dictionary that maps column name to value.

        Parameters:
        • columns: columns to return, or None for all columns
        • where: statement containing WHERE expression, or None to match all rows
        • order: column names to order by, or None to not order results
        • limit: limit the number of results returned, or None to not limit
        • offset: number of rows to skip, or None to skip none

        This coroutine must be called within a database transaction.
        """

        if isinstance(columns, str):
            columns = columns.replace(",", " ").split()

        if order is not None and not isinstance(order, str):
            order = ", ".join(order)

        result = TypedDict(
            "Columns",
            {column: self.columns[column] for column in columns} if columns else self.columns,
        )

        stmt = Statement()
        stmt.text("SELECT ")
        stmt.text(", ".join(typing.get_type_hints(result, include_extras=True).keys()))
        stmt.text(f" FROM {self.name}")
        if where:
            stmt.text(" WHERE ")
            stmt.statement(where)
        if order:
            stmt.text(" ORDER BY ")
            stmt.text(order)
        if limit is not None:
            stmt.text(f" LIMIT {limit}")
        if offset:
            stmt.text(f" OFFSET {offset}")
        stmt.text(";")
        stmt.result = result
        return await self.database.execute(stmt)

    async def count(self, where: Statement = None) -> int:
        """
        Return the number of rows in the table that match an optional expression.

        Parameters:
        • where: statement containing expression to match; None to match all rows
        """

        stmt = Statement()
        stmt.text(f"SELECT COUNT(*) AS count FROM {self.name}")
        if where:
            stmt.text(" WHERE ")
            stmt.statement(where)
        stmt.text(";")
        stmt.result = TypedDict("Result", {"count": int})
        result = await self.database.execute(stmt)
        return (await result.__anext__())["count"]

    async def insert(self, value: Any) -> None:
        """Insert table row."""
        stmt = Statement()
        stmt.text(f"INSERT INTO {self.name} (")
        stmt.text(", ".join(self.columns))
        stmt.text(") VALUES (")
        stmt.parameters(
            (
                Parameter(getattr(value, name), python_type)
                for name, python_type in self.columns.items()
            ),
            ", ",
        )
        stmt.text(");")
        await self.database.execute(stmt)

    async def read(self, key: Any) -> Any:
        """Return a table row, or None if not found."""
        where = Statement()
        where.text(f"{self.pk} = ")
        where.param(key, self.columns[self.pk])
        results = await self.select(where=where, limit=1)
        try:
            return self.schema(**await results.__anext__())
        except StopAsyncIteration:
            return None

    async def update(self, value: Any) -> None:
        """Update table row."""
        key = getattr(value, self.pk)
        stmt = Statement()
        stmt.text(f"UPDATE {self.name} SET ")
        updates = []
        for name, python_type in self.columns.items():
            update = Statement()
            update.text(f"{name} = ")
            update.param(getattr(value, name), python_type)
            updates.append(update)
        stmt.statements(updates, ", ")
        stmt.text(f" WHERE {self.pk} = ")
        stmt.param(key, self.columns[self.pk])
        await self.database.execute(stmt)

    async def delete(self, key: Any) -> None:
        """Delete table row."""
        stmt = Statement()
        stmt.text(f"DELETE FROM {self.name} WHERE {self.pk} = ")
        stmt.param(key, self.columns[self.pk])
        stmt.text(";")
        await self.database.execute(stmt)


class Index:
    """
    Represents an index on a table in a SQL database.

    Parameters:
    • name: name of index
    • table: table that the index defined for
    • keys: index keys (typically column names with optional order)
    """

    __slots__ = ("name", "table", "keys", "unique")

    def __init__(
        self,
        name: str,
        table: Table,
        keys: Sequence[str],
        unique: bool = False,
    ):
        self.name = name
        self.table = table
        self.keys = keys
        self.unique = unique

    def __repr__(self):
        return f"Index(name={self.name}, table={self.table}, keys={self.keys}, unique={self.unique})"

    async def create(self):
        """Create index in database."""
        stmt = Statement()
        stmt.text("CREATE ")
        if self.unique:
            stmt.text("UNIQUE ")
        stmt.text(f"INDEX {self.name} on {self.table.name} (")
        stmt.text(", ".join(self.keys))
        stmt.text(");")
        await self.table.database.execute(stmt)

    async def drop(self):
        """Drop index from database."""
        stmt = Statement()
        stmt.text(f"DROP INDEX {self.name};")
        await self.table.database.execute(stmt)


def row_resource_class(
    table: Table,
    cache_size: int = 0,
    cache_expire: Union[int, float] = 1,
) -> type:
    """
    Return a base class for a row resource.

    Parameters:
    • table: table for which row resource is based
    • cache_size: number of rows to cache (evict least recently cached)
    • cache_expire: expire time for cached values in seconds
    """

    pk_type = table.columns[table.pk]

    cache = (
        memory_resource(
            key_type=pk_type,
            value_type=table.schema,
            size=cache_size,
            evict=True,
            expire=cache_expire,
        )
        if cache_size
        else None
    )

    @resource
    class RowResource:
        """Row resource."""

        def __init__(self, pk: pk_type):
            self.table = table
            self.pk = pk

        async def _validate(self, value: table.schema):
            """Validate value; raise BadRequestError if invalid."""
            pass  # implement in subclass

        async def _read(self):
            if cache:
                with suppress(NotFoundError):
                    return await cache[self.pk].get()
            row = await table.read(self.pk)
            if not row:
                raise NotFoundError
            if cache:
                await cache[self.pk].put(row)
            return row

        async def _insert(self, value: table.schema):
            if getattr(value, table.pk) != self.pk:
                raise BadRequestError("primary key mismatch")
            await table.insert(value)
            if cache:
                await cache[self.pk].put(value)

        async def _update(self, old: table.schema, new: table.schema):
            if getattr(new, table.pk) != self.pk:
                raise BadRequestError("cannot modify primary key")
            if old != new:
                stmt = Statement()
                stmt.text(f"UPDATE {table.name} SET ")
                updates = []
                for name, python_type in table.columns.items():
                    ofield = getattr(old, name)
                    nfield = getattr(new, name)
                    if ofield != nfield:
                        update = Statement()
                        update.text(f"{name} = ")
                        update.param(nfield, python_type)
                        updates.append(update)
                stmt.statements(updates, ", ")
                stmt.text(f" WHERE {table.pk} = ")
                stmt.param(self.pk, pk_type)
                await table.database.execute(stmt)
            if cache:
                await cache[self.pk].put(new)

        @operation
        async def get(self) -> table.schema:
            """Get row from table."""
            if cache:
                with suppress(NotFoundError):
                    return await cache[self.pk].get()
            async with table.database.transaction():
                row = await table.read(self.pk)
            if not row:
                raise NotFoundError
            if cache:
                await cache[self.pk].put(row)
            return row

        @operation
        async def put(self, value: table.schema):
            """Insert or update (upsert) row."""
            await self._validate(value)
            async with table.database.transaction():
                try:
                    old = await self._read()
                    await self._update(old, value)
                except NotFoundError:
                    await self._insert(value)

        @operation
        async def patch(self, body: dict[str, Any]):
            """Modify row. Patch body is a JSON Merge Patch document."""
            async with table.database.transaction():
                old = await self._read()
                with fondat.error.replace((TypeError, ValueError), BadRequestError):
                    new = json_merge_patch(value=old, type=table.schema, patch=body)
                    await self._validate(new)
                await self._update(old, new)

        @operation
        async def delete(self) -> None:
            """Delete row."""
            if cache:
                with suppress(NotFoundError):
                    await cache[self.pk].delete()
            async with table.database.transaction():
                await table.delete(self.pk)

        @query
        async def exists(self) -> bool:
            """Return if row exists."""
            if cache:
                with suppress(NotFoundError):
                    await cache[self.pk].get()
                    return True
            where = Statement()
            where.text(f"{table.pk} = ")
            where.param(self.pk, table.columns[table.pk])
            async with table.database.transaction():
                return await table.count(where) != 0

    fondat.types.affix_type_hints(RowResource, localns=locals())
    return RowResource


def table_resource_class(table: Table, row_resource_type: type = None) -> type:
    """
    Return a base class for a table resource.

    Parameters:
    • table: table for which table resource is based
    • row_resource_type: type to instantiate for table row resource  [implict]
    """

    if row_resource_type is None:
        row_resource_type = row_resource_class(table)

    @datacls
    class Page:
        items: list[table.schema]
        cursor: Optional[bytes] = None

    fondat.types.affix_type_hints(Page, localns=locals())

    dc_codec = get_codec(JSON, table.schema)
    pk_type = table.columns[table.pk]
    pk_codec = get_codec(JSON, pk_type)
    cursor_codec = get_codec(Binary, pk_type)

    class TableResource:
        """Table resource."""

        def __getitem__(self, pk: pk_type) -> row_resource_type:
            return row_resource_type(pk)

        def __init__(self):
            self.table = table

        @operation
        async def get(self, limit: int = None, cursor: bytes = None) -> Page:
            """Get paginated list of rows, ordered by primary key."""
            if cursor is not None:
                where = Statement()
                where.text("{table.pk} > ")
                where.param(pk_type, cursor_codec.decode(cursor))
            else:
                where = None
            async with table.database.transaction():
                results = await table.select(order=table.pk, limit=limit, where=where)
                items = [table.schema(**result) async for result in results]
                cursor = (
                    cursor_codec.encode(getattr(items[-1], table.pk))
                    if limit and len(items)
                    else None
                )
            return Page(items=items, cursor=cursor)

        @operation
        async def patch(self, body: Iterable[dict[str, Any]]):
            """
            Insert and/or modify multiple rows in a single transaction.

            Patch body is an iterable of JSON Merge Patch documents; each document must
            contain the primary key of the row to patch.
            """
            async with table.database.transaction():
                for doc in body:
                    with fondat.error.replace((TypeError, ValueError), BadRequestError):
                        pk = doc.get(table.pk)
                        if pk is None:
                            raise ValueError(f"missing primary key: {table.pk}")
                        row = row_resource_type(pk_codec.decode(pk))
                        try:
                            old = await row._read()
                            new = json_merge_patch(value=old, type=table.schema, patch=doc)
                            await row._validate(new)
                            await row._update(old, new)
                        except NotFoundError:
                            new = dc_codec.decode(doc)
                            validate(new, table.schema)
                            await row._validate(new)
                            await row._insert(new)

    fondat.types.affix_type_hints(TableResource, localns=locals())
    return TableResource


def connection(wrapped=None, *, database: Database):
    """Decorate a coroutine function with a database connection context."""

    if wrapped is None:
        return functools.partial(connection, database=database)

    @wrapt.decorator
    async def wrapper(wrapped, instance, args, kwargs):
        async with database.connection():
            return await wrapped(*args, **kwargs)

    return wrapper(wrapped)


def transaction(wrapped=None, *, database: Database):
    """Decorate a coroutine function with a database transaction context."""

    if wrapped is None:
        return functools.partial(transaction, database=database)

    @wrapt.decorator
    async def wrapper(wrapped, instance, args, kwargs):
        async with database.transaction():
            return await wrapped(*args, **kwargs)

    return wrapper(wrapped)
