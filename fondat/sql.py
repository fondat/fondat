"""Module to manage resource items in a SQL database."""

from __future__ import annotations

import fondat.patch
import logging
import typing

from collections.abc import AsyncIterator, Iterable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, is_dataclass, make_dataclass
from fondat.resource import resource, operation
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
    • value: The value of the parameter to be included.
    • python_type: The type of the pameter to be included.
    """

    value: Any
    python_type: Any


class Statement(Iterable):
    """
    Represents a SQL statement.

    Attributes:
    • result: The type to return a query result row in.

    The result can be expressed as a dataclass to be instantiated, or as a
    TypedDict that results in a populated dict object.
    """

    def __init__(self):
        self.fragments = []
        self.result = None

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
        • value: Parameter value to be appended.
        • python_type: Parameter type; inferred from value if None.
        """
        self.fragments.append(
            Parameter(value, python_type if python_type else type(value))
        )

    def parameter(self, parameter: Parameter) -> None:
        """
        Append a parameter to the statement.
        """
        self.fragments.append(parameter)

    def parameters(self, params: Iterable[Parameter], separator: str = None) -> None:
        """
        Append parameters to this statement, with optional text separator.

        Parameters:
        • params: Parameters to be appended.
        • separator: Separator between parameters.
        """
        sep = False
        for param in params:
            if sep and separator is not None:
                self.text(separator)
            self.parameter(param)
            sep = True

    def statement(self, statement: Statement) -> None:
        """
        Append a statement to this statement.
        """
        self.fragments += statement.fragments

    def statements(
        self, statements: Iterable[Statement], separator: str = None
    ) -> None:
        """
        Append statements to this statement, with optional text separator.

        Parameters:
        • statements: Statements to be added to the statement.
        • separator: Separator between statements.
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

        A transaction context provides SQL transactional semantics
        (commit/rollback) for statements executed. Upon exit of the transaction
        context, if due to exception, the transaction will be rolled back;
        otherwise the transaction will be committed.

        Creating a nested transaction context within a transaction context has
        no effect; only the outermost transaction context will exhibit
        commit/rollback behavior.
        """
        raise NotImplementedError

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[Any]]:
        """
        Execute a SQL statement.

        A transaction context must first be established in order to execute a
        statement.

        If the statement is a query that expects results, then the type of each
        row to be returned is specified in the statement's "result" attribute;
        rows are accessed via a returned asynchronus iterator.

        Parameter:
        • statement: Statement to be executed.
        """
        raise NotImplementedError

    def get_codec(self, python_type: Any) -> Any:
        """
        Return a codec suitable for encoding/decoding the Python type to a
        corresponding SQL type.
        """
        raise NotImplementedError


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: Name of database table.
    • database: Database where table is managed.
    • schema: Dataclass or TypedDict representing the table schema.
    • pk: Column name of primary key.

    Attributes:
    • columns: Mapping of column names to ther associated types.
    """

    def __init__(self, name: str, database: Database, schema: type, pk: str):
        self.name = name
        self.database = database
        if not is_dataclass(schema):
            raise TypeError("table schema must be a dataclass")
        self.schema = schema
        self.columns = typing.get_type_hints(schema, include_extras=True)
        if pk not in self.columns:
            raise ValueError(f"unknown primary key: {pk}")
        self.pk = pk

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
        columns: Union[Iterable[str], str] = None,
        where: Statement = None,
        order: str = None,
        limit: int = None,
        offset: int = None,
    ) -> AsyncIterator[Any]:
        """
        Return an asynchronous iterable for rows in table that match the where
        expression. Each row result a Mapping of column name to value.

        Parameters:
        • columns: Columns to return, or None for all columns.
        • where: Statement containing WHERE expression, or None to match all rows.
        • order: Column names to order by, or None to not order results.
        • limit: Limit the number of results returned, or None to not limit.
        • offset: Number of rows to skip, or None to skip none.

        Columns can be specified as an iterable of column names, or as a string
        containing comma-separated names.
        """

        if isinstance(columns, str):
            columns = columns.replace(",", " ").split()

        result = TypedDict(
            "Columns",
            {column: self.columns[column] for column in columns}
            if columns
            else self.columns,
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
            stmt.text(", ".join(order))
        if limit is not None:
            stmt.text(f" LIMIT {limit}")
        if offset:
            stmt.text(f" OFFSET {offset}")
        stmt.text(";")
        stmt.result = result
        return await self.database.execute(stmt)

    async def count(self, where: Statement = None) -> int:
        """
        Return the number of rows in the table that match an optional
        expression.

        Parameters:
        • where: Statement containing expression to match; None to match all rows.
        """

        stmt = Statement()
        stmt.text(f"SELECT COUNT(*) AS count FROM {self.name}")
        if where:
            stmt.text(" WHERE ")
            stmt.statement(where)
        stmt.text(";")
        stmt.result = TypedDict("Count", {"count": int})
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
    • name: Name of index.
    • table: Table that the index defined for.
    • keys: Index keys (typically column names).
    • unique: Is index unique.
    • desc: Are keys to be sorted in descending order.
    """

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
