"""Module to manage data in a SQL database."""

from __future__ import annotations

import builtins
import fondat.error
import fondat.patch
import fondat.security
import fondat.types
import hashlib
import logging
import re
import typing

from collections.abc import AsyncIterator, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass, is_dataclass
from fondat.cache import CacheResource, hash_json
from fondat.codec import BinaryCodec, DecodeError, JSONCodec
from fondat.error import BadRequestError, NotFoundError
from fondat.pagination import Page, PaginationError
from fondat.patch import json_merge_patch
from fondat.resource import operation, query, resource
from fondat.types import is_optional
from fondat.validation import MinValue, ValidationError, validate
from functools import partial
from typing import Annotated, Any, Generic, TypedDict, TypeVar


_logger = logging.getLogger(__name__)


# type variables
Item = TypeVar("Item")  # item type variable
PK = TypeVar("PK")  # primary key type variable
R = TypeVar("R")  # row type variable
T = TypeVar("T")  # generic type variable


@dataclass
class Param:
    """
    A parameterized value to include in an expression.

    Attributes:
    • value: the value of the parameter
    • type: the Python type of the parameter

    If the type of the value is not specified, then it is inferred by inspecting the
    value's type.
    """

    __slots__ = {"value", "type"}

    def __init__(self, value: Any, type: Any = None):
        self.value = value
        self.type = type if type else builtins.type(value)

    def __repr__(self) -> str:
        return f"Param(value={self.value}, type={self.type})"

    def __str__(self) -> str:
        return f"«{self.value}»"


Fragment = str | Param


class Expression(Iterable[Fragment]):
    """
    Represents a SQL expression as a sequence of fragments.

    Each fragment is one of: a string or Param object.

    An expression can be initialized with multiple arguments, each argument being any of:
    a string, a Param, an Expression, or an Iterable of any of the above.

    An expression can be extended (+=) with any of: a string, a Param, an Expression,
    or an Iterable of any of the above.
    """

    slots = {"fragments"}

    def __init__(self, *args):
        super().__init__()
        self.fragments = []
        for arg in args or ():
            self += arg

    def __repr__(self):
        return f"Expression({self.fragments!r})"

    def __str__(self):
        return "".join(str(f) for f in self.fragments)

    def __iter__(self):
        """Iterate over fragments of the expression."""
        return iter(self.fragments)

    def __bool__(self):
        """Return True if expression contains any fragments."""
        return bool(self.fragments)

    def __len__(self):
        """Return number of fragments in expression."""
        return len(self.fragments)

    def __iadd__(self, value):
        """Add fragment(s) to the expression."""
        match value:
            case str() | Param():
                self.fragments.append(value)
            case Iterable():
                for element in value:
                    self += element  # recursive
            case _:
                raise ValueError
        return self

    def __getitem__(self, key):
        return self.fragments[key]

    @staticmethod
    def join(
        value: Iterable[Expression | Fragment],
        sep: str | None = None,
    ) -> Expression:
        """Join expressions and/or fragments, separated by an optional string."""
        expr = Expression()
        for item in value:
            if expr and sep:
                expr += sep
            expr += item
        return expr


def _to_identifier(value: str):
    if value.isidentifier():
        return value
    return re.sub(r"[^A-Za-z_]", "_", value)


class Database:
    """Base class for a SQL database."""

    async def execute(
        self,
        statement: Expression | str,
        result: type[T] | None = None,
    ) -> AsyncIterator[T] | None:
        """
        Execute a SQL statement.

        Parameter:
        • statement: SQL statement to excute
        • result: the type to return a query result row

        If the statement is a query that generates results, each row can be returned in
        a dataclass or TypedDict object, whose type is specifed in the `result` parameter.
        Rows are provided via a returned asynchronous iterator.

        Must be called within a database transaction context.
        """
        raise NotImplementedError

    async def transaction(self) -> AbstractAsyncContextManager:
        """
        Return an asynchronous context manager, which scopes a transaction in which
        statement(s) are executed. Upon exit of the context, if an exception was raised,
        changes will be rolled back; otherwise changes will be committed.
        """
        raise NotImplementedError

    def sql_type(self, python_type: Any) -> str:
        """Return the SQL type string that corresponds with the specified Python type."""
        raise NotImplementedError


class Table(Generic[R, PK]):
    """
    Represents a table in a SQL database.

    Parameters and attributes:
    • name: name of database table
    • database: database where table is managed
    • schema: dataclass representing the table schema
    • pk: column name of primary key

    Attributes:
    • columns: mapping of column names to ther associated types
    """

    __slots__ = {"name", "database", "schema", "columns", "pk"}

    def __init__(self, name: str, database: Database, schema: type[R], pk: str):
        self.name = name
        self.database = database
        schema = fondat.types.strip_annotations(schema)
        if not is_dataclass(schema):
            raise TypeError("table schema must be a dataclass")
        self.schema = schema
        self.columns = typing.get_type_hints(schema, include_extras=True)
        if pk not in self.columns:
            raise ValueError(f"primary key not in schema: {pk}")
        self.pk = pk

    def __repr__(self):
        return f"Table(name={self.name}, schema={self.schema}, pk={self.pk})"

    async def create(self, execute: bool = True) -> Expression:
        """
        Generate statement to create table in database.

        Parameters:
        • execute: execute statement

        Statement must be executed within a database transaction context.
        """
        stmt = Expression(f"CREATE TABLE {self.name} (")
        columns = []
        for column_name, column_type in self.columns.items():
            column = [column_name, self.database.sql_type(column_type)]
            if column_name == self.pk:
                column.append("PRIMARY KEY")
            if not is_optional(column_type):
                column.append("NOT NULL")
            columns.append(" ".join(column))
        stmt += Expression(", ".join(columns), ");")
        if execute:
            await self.database.execute(stmt)
        return stmt

    async def drop(self, execute: bool = True) -> Expression:
        """
        Generate statement to drop table from database.

        Parameters:
        • execute: execute statement

        Statement must be executed within a database transaction context.
        """
        stmt = Expression(f"DROP TABLE {self.name};")
        if execute:
            await self.database.execute(stmt)
        return stmt

    async def select(
        self,
        *,
        columns: Iterable[str] | str | None = None,
        where: Expression | None = None,
        order_by: Iterable[str] | str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Execute a SQL statement select rows from the table.

        Parameters:
        • columns: name(s) of column(s) to return, or None for all columns
        • where: statement containing WHERE expression, or None to match all rows
        • order_by: names of columns to order rows by, or None to not order rows
        • limit: limit the number of rows returned, or None to not limit
        • offset: number of rows to skip, or None to skip none

        Returns an asynchronous iterable for rows in table that match the where expression.
        Each row item is a dictionary that maps column name to value.

        Must be called within a database transaction context.
        """

        if isinstance(columns, str):
            columns = columns.replace(",", " ").split()

        if columns is None:
            columns = self.columns.keys()

        if order_by is not None and not isinstance(order_by, str):
            order_by = ", ".join(order_by)

        async for row in select_iterator(
            database=self.database,
            columns={column: Expression(column) for column in columns},
            from_=Expression(self.name),
            where=where,
            order_by=Expression(order_by) if order_by is not None else None,
            limit=limit,
            offset=offset,
            row_type=TypedDict("Row", {c: self.columns[c] for c in columns}),
        ):
            yield row

    async def count(self, where: Expression | None = None) -> int:
        """
        Return the number of rows in the table that match an optional expression.
        Must be called within a database transaction context.

        Parameters:
        • where: expression to match; None to match all rows
        """

        stmt = Expression(f"SELECT COUNT(*) AS count FROM {self.name}")
        if where:
            stmt += Expression(" WHERE ", where)
        stmt += ";"
        result = await self.database.execute(stmt, TypedDict("Result", {"count": int}))
        return (await anext(result))["count"]

    async def insert(self, value: R):
        """
        Insert table row. Must be called within a database transaction context.
        """
        stmt = Expression(
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

    async def read(self, pk: PK) -> R:
        """
        Return a table row, or None if not found. Must be called within a database transaction
        context.
        """
        try:
            return self.schema(
                **await anext(
                    self.select(
                        where=Expression(f"{self.pk} = ", Param(pk, self.columns[self.pk])),
                        limit=1,
                    )
                )
            )
        except StopAsyncIteration:
            return None

    async def update(self, value: R):
        """
        Update table row. Must be called within a database transaction context.
        """
        await self.database.execute(
            Expression(
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

    async def delete(self, pk: PK):
        """
        Delete table row. Must be called within a database transaction context.
        """
        await self.database.execute(
            Expression(
                f"DELETE FROM {self.name} WHERE {self.pk} = ",
                Param(pk, self.columns[self.pk]),
                ";",
            )
        )

    async def upsert(self, value: R):
        """
        Upsert table row. Must be called within a database transaction context.
        Default is inefficient; database-specific implementations should override.
        """
        if await self.read(getattr(value, self.pk)) is None:
            await self.insert(value)
        else:
            await self.update(value)


@dataclass
class Index:
    """
    Represents an index on a table in a SQL database.

    Parameters:
    • name: name of index
    • table: table that the index defined for
    • keys: index keys (typically column names with optional order)
    • unique: is index unique
    """

    name: str
    table: Table
    keys: Iterable[str]
    unique: bool = False

    async def create(self, execute: bool = True) -> Expression:
        """
        Generate statement to create index in database.

        Parameters:
        • execute: execute statement

        Statement must be executed within a database transaction context.
        """
        stmt = Expression("CREATE ")
        if self.unique:
            stmt += "UNIQUE "
        stmt += Expression(
            f"INDEX {self.name} ON {self.table.name} (",
            ", ".join(self.keys),
            ");",
        )
        if execute:
            await self.table.database.execute(stmt)
        return stmt

    async def drop(self, execute: bool = True) -> Expression:
        """
        Generate statement to drop index from database.

        Parameters:
        • execute: execute statement

        Statement must be executed within a database transaction context.
        """
        stmt = Expression(f"DROP INDEX {self.name};")
        if execute:
            await self.table.database.execute(stmt)
        return stmt


@resource
class TableResource(Generic[R, PK]):
    def __init__(
        self,
        table: Table,
        cache: CacheResource | None = None,
    ):
        self.table = table
        self.cache = cache

    def __getitem__(self, pk: PK) -> RowResource[R, PK]:
        return RowResource(table=self.table, pk=pk, cache=self.cache)

    @operation
    async def get(
        self,
        limit: Annotated[int, MinValue(1)] = 1000,
        cursor: bytes | None = None,
    ) -> Page[T]:
        """Get paginated list of rows, ordered by primary key."""
        pk_type = self.table.columns[self.table.pk]
        cursor_codec = BinaryCodec.get(pk_type)
        if cursor is not None:
            where = Expression(
                f"{self.table.pk} > ", Param(cursor_codec.decode(cursor), pk_type)
            )
        else:
            where = None
        async with self.table.database.transaction():
            items = [
                self.table.schema(**result)
                async for result in self.table.select(
                    order_by=self.table.pk, limit=limit, where=where
                )
            ]
            cursor = (
                cursor_codec.encode(getattr(items[-1], self.table.pk))
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
        async with self.table.database.transaction():
            for doc in body:
                pk = doc.get(self.table.pk)
                if pk is None:
                    raise ValidationError("missing primary key")
                pk = JSONCodec.get(self.table.columns[self.table.pk]).decode(pk)
                try:
                    await self[pk].patch({k: v for k, v in doc.items() if k != self.table.pk})
                except NotFoundError:
                    new = JSONCodec.get(self.table.schema).decode(doc)
                    validate(new, self.table.schema)
                    await self.table.insert(new)

    @query
    async def find_pks(self, pks: set[PK]) -> list[R]:
        """Return rows corresponding to the specified set of primary keys."""
        if not pks:
            return []
        async with self.table.database.transaction():
            return [
                self.table.schema(**row)
                async for row in self.table.select(
                    where=Expression(
                        f"{self.table.pk} IN (",
                        Expression.join(
                            (Param(pk, self.table.columns[self.table.pk]) for pk in pks), ", "
                        ),
                        ")",
                    )
                )
            ]


@resource
class RowResource(Generic[R, PK]):
    """
    Resource that represents a row in a database table.

    Parameters:
    • table: table where row is located
    • pk: primary key of row
    • cache: resource to cache rows
    """

    def __init__(self, table: Table[R, PK], pk: PK, cache: CacheResource | None = None):
        self.table = table
        self.pk = pk
        self._cache_entry = (
            cache[
                hash_json(
                    {
                        "database": self.table.database.__class__.__qualname__,
                        "table": self.table.name,
                        "pk": JSONCodec.get(self.table.columns[self.table.pk]).encode(self.pk),
                    }
                )
            ]
            if cache
            else None
        )

    @operation
    async def get(self) -> R:
        """Get row from table."""
        if self._cache_entry:
            with suppress(NotFoundError):
                return await self._cache_entry.get()
        async with self.table.database.transaction():
            row = await self.table.read(self.pk)
        if not row:
            raise NotFoundError
        if self._cache_entry:
            await self._cache_entry.put(row)
        return row

    @operation
    async def put(self, value: R):
        """Insert or update (upsert) row."""
        if getattr(value, self.table.pk) != self.pk:
            raise ValidationError("primary key mismatch")
        async with self.table.database.transaction():
            await self.table.upsert(value)
        if self._cache_entry:
            await self._cache_entry.put(value)

    @operation
    async def patch(self, body: dict[str, Any]):
        """Modify row. Patch body is a JSON Merge Patch document."""
        async with self.table.database.transaction():
            if self.table.pk in body:
                raise ValidationError(f"cannot patch field: {self.table.pk}")
            old = await self.table.read(self.pk)
            if not old:
                raise NotFoundError
            try:
                new = json_merge_patch(value=old, type=self.table.schema, patch=body)
            except DecodeError as de:
                raise BadRequestError from de
            validate(new, self.table.schema)
            if old != new:
                stmt = Expression(f"UPDATE {self.table.name} SET ")
                updates = []
                for name, python_type in self.table.columns.items():
                    ofield = getattr(old, name)
                    nfield = getattr(new, name)
                    if ofield != nfield:
                        updates.append(Expression(f"{name} = ", Param(nfield, python_type)))
                stmt += Expression.join(updates, ", ")
                stmt += Expression(
                    f" WHERE {self.table.pk} = ",
                    Param(self.pk, self.table.columns[self.table.pk]),
                    ";",
                )
                await self.table.database.execute(stmt)
            if self._cache_entry:
                await self._cache_entry.put(new)

    @operation
    async def delete(self) -> None:
        """Delete row."""
        if self._cache_entry:
            with suppress(NotFoundError):
                await self._cache_entry.delete()
        async with self.table.database.transaction():
            await self.table.delete(self.pk)

    @query
    async def exists(self) -> bool:
        """Return if row exists."""
        if self._cache_entry:
            with suppress(NotFoundError):
                await self._cache_entry.get()
                return True
        where = Expression(
            f"{self.table.pk} = ", Param(self.pk, self.table.columns[self.table.pk])
        )
        async with self.table.database.transaction():
            return await self.table.count(where) != 0


async def select_iterator(
    *,
    database: Database,
    columns: Mapping[str, Expression],
    from_: Expression | None = None,
    where: Expression | None = None,
    group_by: Expression | None = None,
    having: Expression | None = None,
    order_by: Expression | None = None,
    offset: int | None = None,
    limit: int | None = None,
    row_type: type[T],
) -> AsyncIterator[T]:
    """
    Execute a select statement, returning results through aynchronous row iterator.

    Parameters:
    • columns: mapping of row column names to select expressions
    • from_: FROM expression containing tables to select and/or tables joined
    • where: WHERE condition expression; None to match all rows
    • group_by: GROUP BY expression
    • having: HAVING clause expression
    • order_by: ORDER BY expression
    • offset: number of rows to skip, or None to skip none
    • limit: limit the number of rows returned, or None to select all
    • row_type: type to return for each row

    The row type can be either a dataclass or a typed dictionary. The name of each
    value in the row must correlate with the name of a result column.

    Must be called within a database transaction context.
    """

    stmt = Expression("SELECT ")

    exprs = []
    for name, expr in columns.items():
        exprs.append(
            expr if len(expr) == 1 and expr[0] == name else Expression(expr, f' AS "{name}"')
        )
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

    async for row in await database.execute(stmt, row_type):
        yield row


async def select_page(
    *,
    database: Database,
    columns: Mapping[str, Expression],
    from_: Expression | None = None,
    where: Expression | None = None,
    order_by: Expression,
    limit: int = 1000,
    cursor: bytes | None = None,
    item_type: type[Item],
) -> Page[Item]:
    """
    Execute a select statement, paginating results.

    Parameters:
    • columns: mapping of item column names to select expressions
    • from_: FROM expression containing tables to select and/or tables joined
    • where: WHERE condition expression; None to match all rows
    • order_by: ORDER BY expression
    • limit: limit the number of rows returned, or None to select all
    • item_type: type to return for each item in the page

    The item type can be either a dataclass or a typed dictionary. The name of each value in
    the item must correlate with the name of a result column. In order for pagination to
    operate safely, each returned item must be unique.

    Must be called within a database transaction context.
    """

    cursor_codec = BinaryCodec.get(tuple[int, bytes])
    item_codec = BinaryCodec.get(item_type)

    def hash_item(item: Item) -> bytes:
        return hashlib.sha256(item_codec.encode(item)).digest()

    (offset, hash) = cursor_codec.decode(cursor) if cursor else (None, None)

    _select = partial(
        select_iterator,
        database=database,
        columns=columns,
        from_=from_,
        where=where,
        order_by=order_by,
        row_type=item_type,
    )

    items = [item async for item in _select(offset=offset, limit=limit + 1)]

    if not items or (hash and hash != hash_item(items[0])):  # page drift
        items = []
        found = False
        async for item in _select():  # iterate all
            if not found:
                h = hash_item(item)
                if hash == h:  # index found
                    found = True
            if found:
                items.append(item)
                if len(items) > limit:
                    break
        if not items:
            raise PaginationError("pagination index lost")

    if len(items) > limit:
        hash = hash_item(items[-1])
        del items[-1]
        offset = (offset or 0) + len(items)
        cursor = cursor_codec.encode((offset, hash))
    else:
        cursor = None

    return Page[item_type](items, cursor)


def table_resource_class(
    table: Table,
    row_resource_type: Any = None,
) -> Any:
    """Deprecated. Use TableResource."""

    if not row_resource_type:
        row_resource_type = row_resource_class(table)

    class DeprecatedTableResource(TableResource):
        def __init__(self):
            super().__init__(table)

        def __getitem__(self, pk: Any) -> RowResource:
            return row_resource_type(pk)

    return DeprecatedTableResource


def row_resource_class(
    table: Table,
    cache_size: int = 0,
    cache_expire: int | float = 1,
) -> Any:
    """Deprecated. Use RowResource."""

    cache = None
    if cache_size:
        from fondat.memory import MemoryResource

        cache = MemoryResource(
            key_type=bytes,
            value_type=Any,
            size=cache_size,
            evict=True,
            expire=cache_expire,
        )

    class DeprecatedRowResource(RowResource):
        def __init__(self, pk: Any):
            super().__init__(table=table, pk=pk, cache=cache)

    return DeprecatedRowResource


async def select(
    *,
    database: Database,
    columns: Iterable[tuple[Expression, str, Any]],
    from_: Expression | None = None,
    where: Expression | None = None,
    group_by: Expression | None = None,
    having: Expression | None = None,
    order_by: Expression | None = None,
    offset: int | None = None,
    limit: int | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Deprecated. Use select_iterator or select_page function."""

    cols = {}
    for column in columns:
        name = _to_identifier(column[1])
        while name in cols:
            name += "_"
        cols[name] = column
    stmt = Expression("SELECT ")
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
    rows = await database.execute(
        stmt, TypedDict("Columns", {k: v[2] for k, v in cols.items()})
    )
    async for row in rows:
        yield {cols[k][1]: v for k, v in row.items()}
