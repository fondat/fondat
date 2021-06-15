"""Module to manage data in a SQL database."""

from __future__ import annotations

import fondat.error
import fondat.patch
import fondat.security
import fondat.types
import logging
import typing

from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, is_dataclass
from fondat.codec import get_codec, Binary
from fondat.data import datacls
from fondat.resource import resource, operation, query
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

    async def transaction(self):
        """
        Return context manager that manages a database transaction.

        A transaction context provides SQL transactional semantics (commit/rollback) around
        statements executed.

        Creating an inner transaction context within an outer transaction context has no
        effect; only the outermost transaction context will exhibit commit/rollback behavior.

        Upon exit of the outer transaction context, if an exception was raised, the
        transaction will be rolled back; otherwise the transaction will be committed.
        """
        raise NotImplementedError

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[Any]]:
        """
        Execute a SQL statement.

        A transaction context must be established in order to execute a statement.

        If the statement is a query that expects results, then the type of each row to be
        returned is specified in the statement's "result" attribute; rows are accessed via a
        returned asynchronus iterator.

        Parameter:
        • statement: statement to excute
        """
        raise NotImplementedError

    def get_codec(self, python_type: Any) -> Any:
        """
        Return a codec suitable for encoding/decoding the Python type to a corresponding SQL
        type.
        """
        raise NotImplementedError


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: name of database table
    • database: database where table is managed
    • schema: dataclass or TypedDict representing the table schema
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
        async with self.database.transaction():
            await self.database.execute(stmt)

    async def drop(self):
        """Drop table from database."""
        stmt = Statement()
        stmt.text(f"DROP TABLE {self.name};")
        async with self.database.transaction():
            await self.database.execute(stmt)

    async def select(
        self,
        *,
        columns: Union[Iterable[str], str] = None,
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
        async with self.database.transaction():
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
        async with self.database.transaction():
            await self.database.execute(stmt)

    async def read(self, key: Any) -> Any:
        """Return a table row, or None if not found."""
        where = Statement()
        where.text(f"{self.pk} = ")
        where.param(key, self.columns[self.pk])
        async with self.database.transaction():
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
        async with self.database.transaction():
            await self.database.execute(stmt)

    async def delete(self, key: Any) -> None:
        """Delete table row."""
        stmt = Statement()
        stmt.text(f"DELETE FROM {self.name} WHERE {self.pk} = ")
        stmt.param(key, self.columns[self.pk])
        stmt.text(";")
        async with self.database.transaction():
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
        keys: Iterable[str],
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
        async with self.table.database.transaction():
            await self.table.database.execute(stmt)

    async def drop(self):
        """Drop index from database."""
        stmt = Statement()
        stmt.text(f"DROP INDEX {self.name};")
        async with self.table.database.transaction():
            await self.table.database.execute(stmt)


def row_resource_class(
    table: Table,
    policies: Iterable[fondat.security.Policy] = None,
) -> type:
    """Return a base class for a row resource."""

    pk_type = table.columns[table.pk]

    @resource
    class RowResource:
        """Row resource."""

        def __init__(self, pk: pk_type):
            self.table = table
            self.pk = pk

        @operation(policies=policies)
        async def get(self) -> table.schema:
            if row := await table.read(self.pk):
                return row
            raise fondat.error.NotFoundError

        @operation(policies=policies)
        async def put(self, value: table.schema):
            """Insert or update (upsert) row."""
            if getattr(value, table.pk) != self.pk:
                raise fondat.error.BadRequestError("value pk must match resource pk")
            if not await self.exists():
                await table.insert(value)
            else:
                await table.update(value)

        @operation(policies=policies)
        async def patch(self, body: dict[str, Any]):
            """Modify row."""
            if table.pk in body:
                raise fondat.error.BadRequestError(f"cannot patch field: {table.pk}")
            old = await self.get()
            new = fondat.patch.json_merge_patch(value=old, type=table.schema, patch=body)
            if old == new:  # nothing to update
                return
            stmt = Statement()
            stmt.text(f"UPDATE {table.name} SET ")
            updates = []
            for name, python_type in table.columns.items():
                if getattr(old, name) != getattr(new, name):
                    update = Statement()
                    update.text(f"{name} = ")
                    update.param(getattr(new, name), python_type)
                    updates.append(update)
            stmt.statements(updates, ", ")
            stmt.text(f" WHERE {table.pk} = ")
            stmt.param(self.pk, pk_type)
            async with table.database.transaction():
                await table.database.execute(stmt)

        @operation(policies=policies)
        async def delete(self) -> None:
            """Delete row."""
            await table.delete(self.pk)

        @query(policies=policies)
        async def exists(self) -> bool:
            """Return if row exists."""
            where = Statement()
            where.text(f"{table.pk} = ")
            where.param(self.pk, table.columns[table.pk])
            return await table.count(where) != 0

    fondat.types.affix_type_hints(RowResource, localns=locals())
    return RowResource


def table_resource_class(
    table: Table,
    row_resource_type: type,
    policies: Iterable[fondat.security.Policy] = None,
) -> type:
    """Return a base class for a table resource."""

    @datacls
    class Page:
        items: list[table.schema]
        cursor: Optional[bytes] = None

    fondat.types.affix_type_hints(Page, localns=locals())

    pk_type = table.columns[table.pk]
    cursor_codec = get_codec(Binary, pk_type)

    class TableResource:
        """Table resource."""

        def __getitem__(self, pk: pk_type) -> row_resource_type:
            return row_resource_type(pk)

        def __init__(self):
            self.table = table

        @operation(policies=policies)
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

    fondat.types.affix_type_hints(TableResource, localns=locals())
    return TableResource
