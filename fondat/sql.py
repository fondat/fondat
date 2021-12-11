"""Module to manage data in a SQL database."""

from __future__ import annotations

import builtins
import fondat.error
import fondat.patch
import fondat.security
import fondat.types
import logging
import re
import typing

from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass, is_dataclass
from fondat.codec import get_codec, Binary, JSON
from fondat.data import datacls
from fondat.error import BadRequestError, NotFoundError
from fondat.memory import memory_resource
from fondat.patch import json_merge_patch
from fondat.resource import resource, operation, query
from fondat.validation import MinValue, validate
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


class Expression(Iterable):
    """Represents a SQL expression."""

    slots = ("fragments",)

    def __init__(self, *fragments):
        super().__init__()
        self.fragments = []
        for fragment in fragments or ():
            self += fragment

    def __repr__(self) -> str:
        return f"Expression({self.fragments})"

    def __str__(self) -> str:
        return "".join(str(f) for f in self.fragments)

    def __iter__(self) -> Iterator[Any]:
        """Iterate over fragments of the statement."""
        return iter(self.fragments)

    def __bool__(self) -> bool:
        """Return True if expression contains fragments."""
        return len(self.fragments) > 0

    def __len__(self) -> int:
        """Return number of fragments in expression."""
        return len(self.fragments)

    def __iadd__(self, value: Any) -> None:
        """Add a fragment to the expression."""
        if isinstance(value, Expression):
            self.fragments.extend(value.fragments)
        else:
            self.fragments.append(value)
        return self

    @staticmethod
    def join(value: Iterable[Any], sep: str = None) -> Expression:
        """Join a number of fragments, optionally separated by a string value."""
        expr = Expression()
        for item in value:
            if expr and sep:
                expr += sep
            expr += item
        return expr


class Statement(Expression):
    """
    A SQL statement: a SQL expression with an optional expected row result type.

    Parameter and attribute:
    • result: the type to return a query result row in

    The result can be expressed as a dataclass to be instantiated, or as a TypedDict that
    results in a populated dict object.
    """

    slots = ("result",)

    def __init__(self, *fragments, result: Any = None):
        super().__init__(*fragments)
        self.result = result

    def __repr__(self):
        return f"Statement(fragments={self.fragments}, result={self.result})"


@dataclass
class Param:
    """
    A parameterized value to include in an expression.

    Attributes:
    • value: the value of the parameter to be included
    • python_type: the type of the pameter to be included
    """

    __slots__ = ("value", "type")

    def __init__(self, value: Any, type: Any = None):
        self.value = value
        self.type = type if type else builtins.type(value)

    def __repr__(self) -> str:
        return f"Param(value={self.value}, type={self.type})"

    def __str__(self) -> str:
        return repr(self)


def _to_identifier(value: str):
    if value.isidentifier():
        return value
    return re.sub(r"[^A-Za-z_]", "_", value)


class Database:
    """Base class for a SQL database."""

    async def connection(self):
        """
        Return a context manager that establishes a connection to the database. If a
        connection context has already been established for the current task, this
        method has no effect. Upon exit of the outermost connection context, the
        database connection is closed.
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

    async def execute(self, statement: Statement) -> Optional[AsyncIterator[dict[str, Any]]]:
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

    async def select(
        self,
        *,
        columns: Sequence[tuple[Expression, str, Any]],
        from_: Expression = None,
        where: Expression = None,
        group_by: Expression = None,
        having: Expression = None,
        order_by: Expression = None,
        limit: int = None,
        offset: int = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Execute a select statement.

        Parameters:
        • columns: tuple of (key, expression, type)
        • from_: FROM expression containing tables to select and/or tables joined
        • where: WHERE condition expression; None to match all rows
        • group_by: GROUP BY expression
        • having: HAVING clause expression
        • order_by: ORDER BY expression
        • limit: limit the number of results returned, or None to not limit
        • offset: number of rows to skip, or None to skip none

        Each column specified is a tuple with the values in this order:
        • expression: expression for the SQL database to process
        • alias: column alias; key to store the value in the row dictionary
        • type: the Python type in which to store the evaluated expression

        Returns an asynchronous iterable containing rows; each row item is a dictionary that
        maps column name to evaluated expression value.

        This coroutine must be called within a database transaction.
        """

        cols = {}
        for column in columns:
            name = _to_identifier(column[1])
            while name in cols:
                name += "_"
            cols[name] = column
        result = TypedDict("Columns", {k: v[2] for k, v in cols.items()})
        stmt = Statement("SELECT ")
        exprs = []
        for k, v in cols.items():
            expr = Expression(v[0])
            if len(v[0]) != 1 or k != v[0].fragments[0]:
                expr += f" AS {k}"
            exprs.append(expr)
        stmt += Expression.join(exprs, ", ")
        if from_ is not None:
            stmt += Expression(" FROM ", from_)
        if where is not None:
            stmt += Expression(" WHERE ", where)
        if group_by is not None:
            stmt += Expression(" GROUP BY ", group_by)
        if having is not None:
            stmt += Expression(" HAVING ", having)
        if order_by is not None:
            stmt += Expression(" ORDER BY ", order_by)
        if limit:
            stmt += f" LIMIT {limit}"
        if offset:
            stmt += f" OFFSET {offset}"
        stmt += ";"
        stmt.result = result
        results = await self.execute(stmt)
        async for row in results:
            yield {cols[k][1]: v for k, v in row.items()}


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
        stmt += f"CREATE TABLE {self.name} ("
        columns = []
        for column_name, column_type in self.columns.items():
            column = [column_name, self.database.get_codec(column_type).sql_type]
            if column_name == self.pk:
                column.append("PRIMARY KEY")
            if not is_nullable(column_type):
                column.append("NOT NULL")
            columns.append(" ".join(column))
        stmt += Expression(", ".join(columns), ");")
        await self.database.execute(stmt)

    async def drop(self):
        """Drop table from database."""
        await self.database.execute(Statement(f"DROP TABLE {self.name};"))

    async def select(
        self,
        *,
        columns: Union[Sequence[str], str] = None,
        where: Expression = None,
        order_by: Union[Sequence[str], str] = None,
        limit: int = None,
        offset: int = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Execute a SQL statement select rows from the table.

        Parameters:
        • columns: columns to return, or None for all columns
        • where: statement containing WHERE expression, or None to match all rows
        • order_by: column names to order by, or None to not order results
        • limit: limit the number of results returned, or None to not limit
        • offset: number of rows to skip, or None to skip none

        Returns an asynchronous iterable for rows in table that match the where expression.
        Each row item is a dictionary that maps column name to value.

        This coroutine must be called within a database transaction.
        """

        if isinstance(columns, str):
            columns = columns.replace(",", " ").split()

        if order_by is not None and not isinstance(order_by, str):
            order_by = ", ".join(order_by)

        result = TypedDict(
            "Columns",
            {column: self.columns[column] for column in columns} if columns else self.columns,
        )

        async for row in self.database.select(
            columns=[
                (Expression(key), key, type)
                for key, type in typing.get_type_hints(result, include_extras=True).items()
            ],
            from_=Expression(self.name),
            where=where,
            order_by=Expression(order_by) if order_by is not None else None,
            limit=limit,
            offset=offset,
        ):
            yield row

    async def count(self, where: Expression = None) -> int:
        """
        Return the number of rows in the table that match an optional expression.

        Parameters:
        • where: expression to match; None to match all rows
        """

        stmt = Statement(f"SELECT COUNT(*) AS count FROM {self.name}")
        if where:
            stmt += Expression(" WHERE ", where)
        stmt += ";"
        stmt.result = TypedDict("Result", {"count": int})
        result = await self.database.execute(stmt)
        return (await result.__anext__())["count"]

    async def insert(self, value: Any) -> None:
        """Insert table row."""
        stmt = Statement(
            f"INSERT INTO {self.name} (",
            ", ".join(self.columns),
            ") VALUES (",
            Expression.join(
                (
                    Param(getattr(value, name), python_type)
                    for name, python_type in self.columns.items()
                ),
                ", ",
            ),
            ");",
        )
        await self.database.execute(stmt)

    async def read(self, key: Any) -> Any:
        """Return a table row, or None if not found."""
        try:
            return self.schema(
                **await self.select(
                    where=Expression(f"{self.pk} = ", Param(key, self.columns[self.pk])),
                    limit=1,
                ).__anext__()
            )
        except StopAsyncIteration:
            return None

    async def update(self, value: Any) -> None:
        """Update table row."""
        await self.database.execute(
            Statement(
                f"UPDATE {self.name} SET ",
                Expression.join(
                    (
                        Expression(f"{name} = ", Param(getattr(value, name), python_type))
                        for name, python_type in self.columns.items()
                    ),
                    ", ",
                ),
                f" WHERE {self.pk} = ",
                Param(getattr(value, self.pk), self.columns[self.pk]),
                ";",
            )
        )

    async def delete(self, key: Any) -> None:
        """Delete table row."""
        await self.database.execute(
            Statement(
                f"DELETE FROM {self.name} WHERE {self.pk} = ",
                Param(key, self.columns[self.pk]),
                ";",
            )
        )


class Index:
    """
    Represents an index on a table in a SQL database.

    Parameters:
    • name: name of index
    • table: table that the index defined for
    • keys: index keys (typically column names with optional order)
    • unique: is index unique
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
        stmt = Statement("CREATE ")
        if self.unique:
            stmt += "UNIQUE "
        stmt += Expression(
            f"INDEX {self.name} ON {self.table.name} (",
            ", ".join(self.keys),
            ");",
        )
        await self.table.database.execute(stmt)

    async def drop(self):
        """Drop index from database."""
        await self.table.database.execute(Statement(f"DROP INDEX {self.name};"))


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
                stmt = Statement(f"UPDATE {table.name} SET ")
                updates = []
                for name, python_type in table.columns.items():
                    ofield = getattr(old, name)
                    nfield = getattr(new, name)
                    if ofield != nfield:
                        updates.append(Expression(f"{name} = ", Param(nfield, python_type)))
                stmt += Expression.join(updates, ", ")
                stmt += Expression(f" WHERE {table.pk} = ", Param(self.pk, pk_type), ";")
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
            where = Expression(f"{table.pk} = ", Param(self.pk, table.columns[table.pk]))
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
        async def get(
            self,
            limit: Annotated[int, MinValue(1)] = 1000,
            cursor: bytes = None,
        ) -> Page:
            """Get paginated list of rows, ordered by primary key."""
            if cursor is not None:
                where = Expression(
                    f"{table.pk} > ", Param(cursor_codec.decode(cursor), pk_type)
                )
            else:
                where = None
            async with table.database.transaction():
                items = [
                    table.schema(**result)
                    async for result in table.select(
                        order_by=table.pk, limit=limit, where=where
                    )
                ]
                cursor = (
                    cursor_codec.encode(getattr(items[-1], table.pk))
                    if len(items) == limit
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

        @query
        async def find_pks(self, pks: set[table.columns[table.pk]]) -> list[table.schema]:
            """Return rows corresponding to the specified set of primary keys."""
            if not pks:
                return []
            async with table.database.transaction():
                return [
                    table.schema(**row)
                    async for row in table.select(
                        where=Expression(
                            f"{table.pk} IN (",
                            Expression.join(
                                (Param(pk, table.columns[table.pk]) for pk in pks), ", "
                            ),
                            ")",
                        )
                    )
                ]

    fondat.types.affix_type_hints(TableResource, localns=locals())
    return TableResource
