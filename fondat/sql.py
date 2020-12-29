"""Module to manage resource items in a SQL database."""

from __future__ import annotations

import fondat.patch
import logging
import typing

from collections.abc import AsyncIterable, Iterable, Mapping
from dataclasses import dataclass, is_dataclass
from fondat.resource import resource, operation
from typing import Annotated, Any, Union


_logger = logging.getLogger(__name__)


def is_nullable(py_type):
    """Return if Python type allows for None value."""
    NoneType = type(None)
    if typing.get_origin(py_type) is Annotated:
        py_type = typing.get_args(py_type)[0]  # strip annotation
    if py_type is NoneType:
        return True
    if typing.get_origin(py_type) is not Union:
        return False
    for arg in typing.get_args(py_type):
        if arg is NoneType:
            return True
    return False


@dataclass
class Parameter:
    """
    Represents a parameterized value to include in a statement.

    Attributes:
    • py_type: The type of the pameter to be included.
    • value: The value of the parameter to be included.
    """

    py_type: type
    value: Any


class Statement(Iterable):
    """Represents a SQL statement."""

    def __init__(self):
        self.fragments = []

    def __iter__(self):
        return iter(self.fragments)

    def text(self, value: str) -> None:
        """Append text to the statement."""
        self.fragments.append(value)

    def param(self, value: Parameter) -> None:
        """Append a parameter to the statement."""
        self.fragments.append(value)

    def params(self, params: Iterable[Parameter], separator: str = None) -> None:
        """
        Append parameters to this statement, with optional text separator.

        Parameters:
        • params: Parameters to be added.
        • separator: Separator between parameters.
        """
        sep = False
        for param in params:
            if sep and separator is not None:
                self.text(separator)
            self.param(param)
            sep = True

    def statement(self, statement: Statement) -> None:
        """Append another statement to this statement."""
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


class Transaction:
    """
    Base class for a SQL transaction.

    A transaction object is a context manager that manages a database
    transaction. A transaction provides the means to execute a SQL
    statement, and provides transaction semantics (commit/rollback).

    Upon exit of the context manager, in the event of an exception, the
    transaction will be rolled back; otherwise, the transaction will be
    committed.
    """

    async def execute(self, statement: Statement) -> None:
        """
        Execute a statement with no expected result.

        • statement: Statement to be executed.
        """
        raise NotImplementedError

    async def query(self, query: Statement) -> AsyncIterable[Mapping[str, Any]]:
        """
        Execute a statement with an expected result.

        • query: Query to be executed.

        The returned value is an asynchronus iterator, which iterates over rows
        in the query result set; each value yielded is a mapping of column name
        to value.
        """
        raise NotImplementedError


class Database:
    """Base class for a SQL database."""

    async def transaction(self) -> Transaction:
        """
        Return context manager that manages a database transaction.

        If more than one request for a transaction is made within the same
        task, the same transaction will be returned; only the outermost
        yielded transaction will exhibit commit/rollback behavior.
        """
        raise NotImplementedError

    def get_codec(self, python_type: Any) -> Any:
        """TODO: Description."""
        raise NotImplementedError


class Table:
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: Name of database table.
    • database: Database where table is managed.
    • schema: Data class representing the table schema.
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
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def drop(self):
        """Drop table from database."""
        stmt = Statement()
        stmt.text(f"DROP TABLE {self.name};")
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def select(
        self,
        columns: Union[Iterable[str], str] = None,
        where: Statement = None,
        order: str = None,
        limit: int = None,
        offset: int = None,
    ) -> Iterable[Mapping[str, Any]]:
        """
        Return a list of rows in table that match the where expression. Each
        row result is expressed in a dict-like object.

        Parameters:
        • columns: Column names to return, or None for all columns.
        • where: Statement containing WHERE expression, or None to match all rows.
        • order: Column names to order by, or None to not order results.
        • limit: Limit the number of results returned, or None to not limit.
        • offset: Number of rows to skip, or None to skip none.
        """
        if isinstance(columns, str):
            columns = columns.replace(",", " ").split()
        columns = tuple(columns or self.columns.keys())
        stmt = Statement()
        stmt.text("SELECT ")
        stmt.text(", ".join(columns))
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
        results = []
        async with self.database.transaction() as t:
            async for row in await t.query(stmt):
                results.append(
                    {
                        column: self.database.get_codec(self.columns[column]).decode(
                            row[column]
                        )
                        for column in columns
                    }
                )
        return results

    async def insert(self, value: Any) -> None:
        """Insert table row."""
        stmt = Statement()
        stmt.text(f"INSERT INTO {self.name} (")
        stmt.text(", ".join(self.columns))
        stmt.text(") VALUES (")
        stmt.params(
            (
                Parameter(py_type, getattr(value, name))
                for name, py_type in self.columns.items()
            ),
            ", ",
        )
        stmt.text(");")
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def read(self, key: Any) -> Any:
        """Return a table row, or None if not found."""
        where = Statement()
        where.text(f"{self.pk} = ")
        where.param(Parameter(self.columns[self.pk], key))
        results = await self.select(where=where)
        try:
            kwargs = next(iter(results))
        except StopIteration:
            return None
        return self.schema(**kwargs)

    async def update(self, value: Any) -> None:
        """Update table row."""
        key = getattr(value, self.pk)
        stmt = Statement()
        stmt.text(f"UPDATE {self.name} SET ")
        updates = []
        for name, py_type in self.columns.items():
            update = Statement()
            update.text(f"{name} = ")
            update.param(Parameter(py_type, getattr(value, name)))
            updates.append(update)
        stmt.statements(updates, ", ")
        stmt.text(f" WHERE {self.pk} = ")
        stmt.param(Parameter(self.columns[self.pk], key))
        async with self.database.transaction() as t:
            await t.execute(stmt)

    async def delete(self, key: Any) -> None:
        """Delete table row."""
        stmt = Statement()
        stmt.text(f"DELETE FROM {self.name} WHERE {self.pk} = ")
        stmt.param(Parameter(self.columns[self.pk], key))
        stmt.text(";")
        async with self.database.transaction() as t:
            await t.execute(stmt)


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
        async with self.table.database.transaction() as t:
            await t.execute(stmt)

    async def drop(self):
        """Drop index from database."""
        stmt = Statement()
        stmt.text(f"DROP INDEX {self.name};")
        async with self.transaction() as t:
            await t.execute(stmt)
