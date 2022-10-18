import asyncio
import contextlib
import fondat.error
import fondat.patch
import fondat.sql as sql
import fondat.sqlite as sqlite
import pytest
import tempfile

from dataclasses import dataclass
from datetime import date, datetime
from fondat.data import datacls, make_datacls
from fondat.memory import MemoryResource
from fondat.sql import Expression
from typing import Any, Literal, TypedDict
from uuid import UUID, uuid4


@datacls
class DC:
    key: UUID
    str_: str | None
    dict_: TypedDict("TD", {"a": int | None, "b": int | None}, total=False) | None
    list_: list[int] | None
    set_: set[str] | None
    int_: int | None
    float_: float | None
    bool_: bool | None
    bytes_: bytes | None
    date_: date | None
    datetime_: datetime | None
    tuple_: tuple[str, int, float] | None
    tuple_ellipsis: tuple[int, ...] | None
    str_literal: Literal["a", "b", "c"] | None
    int_literal: Literal[1, 2, 3] | None
    mixed_literal: Literal["a", 1, True] | None


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        yield database


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def table(database):
    foo = sqlite.Table("foo", database, DC, "key")
    async with database.transaction():
        await foo.create()
    yield foo
    async with database.transaction():
        await foo.drop()


async def test_table_crud(table):
    row = DC(
        key=uuid4(),
        str_="string",
        dict_={"a": 1},
        list_=[1, 2, 3],
        set_={"foo", "bar"},
        int_=1,
        float_=2.3,
        bool_=True,
        bytes_=b"12345",
        date_=date.fromisoformat("2019-01-01"),
        datetime_=datetime.fromisoformat("2019-01-01T01:01:01+00:00"),
        tuple_=("a", 1, 2.3),
        tuple_ellipsis=(1, 2, 3),
    )
    async with table.database.transaction():
        await table.insert(row)
        assert await table.read(row.key) == row
        row.dict_ = {"a": 2}
        row.list_ = [2, 3, 4]
        row.set_ = None
        row.int_ = 2
        row.float_ = 1.0
        row.bool_ = False
        row.bytes_ = None
        row.date_ = None
        row.datetime_ = None
        row.tuple_ = None
        row.tuple_ellipsis = None
        await table.update(row)
        assert await table.read(row.key) == row
        await table.delete(row.key)
        assert await table.read(row.key) is None


async def test_fast_upsert(table):
    key = uuid4()
    row = DC(
        key=key,
        str_="string",
    )
    async with table.database.transaction():
        await table.upsert(row)
        read = await table.read(row.key)
        assert read.str_ == "string"
        row.str_ = "bling"
        await table.upsert(row)
        read = await table.read(row.key)
        assert read.str_ == "bling"


async def test_slow_upsert(database):
    table = sql.Table("foo", database, DC, "key")
    async with database.transaction():
        await table.create()
    try:
        key = uuid4()
        row = DC(
            key=key,
            str_="string",
        )
        async with database.transaction():
            await table.upsert(row)
            read = await table.read(row.key)
            assert read.str_ == "string"
            row.str_ = "bling"
            await table.upsert(row)
            read = await table.read(row.key)
            assert read.str_ == "bling"
    finally:
        async with database.transaction():
            await table.drop()


async def test_binary(database):
    @datacls
    class Bin:
        key: UUID
        bin: bytes

    row = Bin(key=uuid4(), bin=b"\x01\x02\x03\x04\x05")
    table = sqlite.Table("bin", database, Bin, "key")
    async with database.transaction():
        await table.create()
        try:
            await table.insert(row)
            assert await table.read(row.key) == row
            row.bin = b"bacon"
            await table.update(row)
            assert (await table.read(row.key)).bin == b"bacon"
        finally:
            await table.drop()


async def test_list(table):
    async with table.database.transaction():
        count = 10
        for _ in range(0, count):
            await table.insert(DC(key=uuid4()))
        assert await table.count() == count
        async for result in table.select(columns="key"):
            await table.delete(result["key"])
        assert await table.count() == 0


async def test_rollback(table):
    key = uuid4()
    try:
        async with table.database.transaction():
            await table.insert(DC(key=key))
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    async with table.database.transaction():
        assert await table.read(key) is None


async def test_index(table):
    index = sql.Index("foo_ix_str", table, ("str_",))
    async with table.database.transaction():
        await index.create()
        await index.drop()


async def test_select_order_by(table):
    async with table.database.transaction():
        await table.insert(DC(key=uuid4(), str_="B", int_=1))
        await table.insert(DC(key=uuid4(), str_="B", int_=2))
        await table.insert(DC(key=uuid4(), str_="A"))
        await table.insert(DC(key=uuid4(), str_="C"))
        keys = [row["str_"] async for row in table.select(order_by="str_")]
        assert keys == ["A", "B", "B", "C"]
        keys = [
            (row["str_"], row["int_"]) async for row in table.select(order_by=["str_", "int_"])
        ]
        assert keys == [("A", None), ("B", 1), ("B", 2), ("C", None)]
        keys = [(row["str_"], row["int_"]) async for row in table.select(order_by="str_, int_")]
        assert keys == [("A", None), ("B", 1), ("B", 2), ("C", None)]


async def test_resource_crud(table):
    pk = uuid4()
    resource = sql.TableResource(table)[pk]
    row = DC(
        key=pk,
        str_="string",
        dict_={"a": 1},
        list_=[1, 2, 3],
        set_={"foo", "bar"},
        int_=1,
        float_=2.3,
        bool_=True,
        bytes_=b"12345",
        date_=date.fromisoformat("2019-01-01"),
        datetime_=datetime.fromisoformat("2019-01-01T01:01:01+00:00"),
        str_literal="a",
        int_literal=2,
        mixed_literal=1,
    )
    await resource.put(row)
    assert await resource.get() == row
    row.dict_ = {"a": 2}
    row.list_ = [2, 3, 4]
    row.set_ = None
    row.int_ = 2
    row.float_ = 1.0
    row.bool_ = False
    row.bytes_ = None
    row.date_ = None
    row.datetime_ = None
    row.str_literal = None
    row.int_literal = None
    row.mixed_literal = None
    await resource.put(row)
    assert await resource.get() == row
    await resource.delete()
    with pytest.raises(fondat.error.NotFoundError):
        await resource.get()


async def test_resource_patch(table):
    pk = uuid4()
    resource = sql.TableResource(table)[pk]
    row = DC(
        key=pk,
        str_="string",
        dict_={"a": 1},
        list_=[1, 2, 3],
        set_={"foo", "bar"},
        int_=1,
        float_=2.3,
        bool_=True,
        bytes_=b"12345",
        date_=date.fromisoformat("2019-01-01"),
        datetime_=datetime.fromisoformat("2019-01-01T01:01:01+00:00"),
    )
    await resource.put(row)
    patch = {"str_": "new_string", "dict_": {"a": None, "b": 2}}
    await resource.patch(patch)
    row = fondat.patch.json_merge_patch(value=row, type=DC, patch=patch)
    assert await resource.get() == row


async def test_resource_patch_small(table):
    pk = uuid4()
    resource = sql.TableResource(table)[pk]
    row = DC(key=pk, str_="string")
    await resource.put(row)
    patch = {"str_": "strung"}
    await resource.patch(patch)
    resource = sql.TableResource(table)[pk]
    row = await resource.get()
    assert row.str_ == "strung"


async def test_resource_put_invalid_pk(table):
    pk = uuid4()
    resource = sql.TableResource(table)[pk]
    row = DC(key=uuid4(), str_="string")  # different pk
    with pytest.raises(fondat.error.BadRequestError):
        await resource.put(row)


async def test_resource_patch_pk(table):
    pk = uuid4()
    resource = sql.TableResource(table)[pk]
    row = DC(key=pk, str_="string")
    await resource.put(row)
    patch = {"key": str(uuid4())}  # modify pk
    with pytest.raises(fondat.error.BadRequestError):
        await resource.patch(patch)


async def test_gather(database):
    async def select(n: int):
        stmt = sql.Expression(f"SELECT {n} AS foo;")
        result = await (
            await database.execute(stmt, make_datacls("DC", (("foo", int),)))
        ).__anext__()
        assert result.foo == n

    async with database.transaction():
        await asyncio.gather(*[select(n) for n in range(0, 50)])


async def test_resource_list(table):
    resource = sql.TableResource(table)
    count = 5
    for n in range(0, count):
        key = uuid4()
        await resource[key].put(DC(key=key, int_=n))
    results = await resource.get()
    assert len(results.items) == count


async def test_nested_transaction(table):
    async with table.database.transaction():
        assert await table.count() == 0
        await table.insert(DC(key=uuid4()))
        assert await table.count() == 1
        try:
            async with table.database.transaction():
                await table.insert(DC(key=uuid4()))
                assert await table.count() == 2
                raise RuntimeError
        except RuntimeError:
            pass
        assert await table.count() == 1


async def test_table_patch(database):
    DC = make_datacls("DC", (("id", str), ("s", str)))
    table = sqlite.Table("dc", database, DC, "id")
    async with database.transaction():
        with contextlib.suppress(Exception):
            await table.drop()
        await table.create()
        await table.insert(DC(id="a", s="aaa"))
        await table.insert(DC(id="b", s="aaa"))
        await table.insert(DC(id="c", s="aaa"))
        resource = sql.TableResource(table)
        await resource.patch(
            [
                {"id": "a", "s": "bbb"},
                {"id": "b", "s": "bbb"},
                {"id": "z", "s": "zzz"},
            ]
        )
        assert await table.read("a") == DC(id="a", s="bbb")
        assert await table.read("b") == DC(id="b", s="bbb")
        assert await table.read("c") == DC(id="c", s="aaa")
        assert await table.read("z") == DC(id="z", s="zzz")
        with pytest.raises(fondat.error.BadRequestError):
            await resource.patch([{"id": "a", "s": 123}])
        await table.drop()


async def test_get_cache(table: sqlite.Table):
    cache = MemoryResource(key_type=bytes, value_type=Any, size=10, expire=10, evict=True)
    key = UUID("14f6a6b0-e4d7-4f3f-bb8c-66076fd6fce9")
    row = DC(key=key, str_=str(key))
    async with table.database.transaction():
        await table.insert(row)
    assert await sql.RowResource(table, key, cache).get() == row  # caches row
    async with table.database.transaction():
        await table.delete(key)
    assert await sql.RowResource(table, key, cache).get() == row  # still cached
    async with table.database.transaction():
        await table.delete(key)


async def test_put_get_cache(table: sqlite.Table):
    cache = MemoryResource(key_type=bytes, value_type=Any, size=10, expire=10, evict=True)
    key = UUID("b616303b-1278-4209-8397-4fab852c8959")
    row = DC(key=key, str_=str(key))
    await sql.RowResource(table, key, cache).put(row)  # caches row
    async with table.database.transaction():
        await table.delete(key)
    assert await sql.RowResource(table, key, cache).get() == row  # still cached


async def test_delete_cache(table: sqlite.Table):
    cache = MemoryResource(key_type=bytes, value_type=Any, size=10, expire=10, evict=True)
    key = UUID("38340a23-e11a-412b-b20a-22dd7fc3d316")
    row = DC(key=key, str_=str(key))
    await sql.RowResource(table, key, cache).put(row)  # caches row
    await sql.RowResource(table, key, cache).delete()  # deletes cached row
    with pytest.raises(fondat.error.NotFoundError):
        await sql.RowResource(table, key, cache).get()


async def test_get_cache_evict(table: sqlite.Table):
    cache = MemoryResource(key_type=bytes, value_type=Any, size=1, expire=10, evict=True)
    key1 = UUID("16ed1e46-a111-414c-b05c-99a8b876afd0")
    row1 = DC(key=key1, str_=str(key1))
    async with table.database.transaction():
        await table.insert(row1)
    key2 = UUID("6bdba737-0401-4d8b-9a22-d8b6b0f8b5b7")
    row2 = DC(key=key2, str_=str(key2))
    async with table.database.transaction():
        await table.insert(row2)
    assert await sql.RowResource(table, key1, cache).get() == row1
    assert await sql.RowResource(table, key2, cache).get() == row2
    async with table.database.transaction():
        await table.delete(key1)
        await table.delete(key2)
    with pytest.raises(fondat.error.NotFoundError):
        await sql.RowResource(table, key1, cache).get() == row1  # evicted
    assert await sql.RowResource(table, key2, cache).get() == row2  # still cached


async def test_exists_no_cache(table):
    key = uuid4()
    resource = sql.RowResource(table, key)
    row = DC(key=key, str_=str(key))
    assert not await resource.exists()
    await resource.put(row)
    assert await resource.exists()


async def test_exists_cache(table):
    cache = MemoryResource(key_type=bytes, value_type=Any, size=10, expire=10, evict=True)
    key = uuid4()
    resource = sql.RowResource(table, key, cache)
    row = DC(key=key, str_=str(key))
    assert not await resource.exists()
    await resource.put(row)
    assert await resource.exists()


async def test_select_iterator(database: sql.Database):
    row_type = TypedDict("Row", {"name.is.here": int})
    async with database.transaction():
        await database.execute(sql.Expression("CREATE TABLE foo (n int);"))
        for n in range(10):
            await database.execute(sql.Expression(f"INSERT INTO foo VALUES ({n});"))
    try:
        async with database.transaction():
            async for row in sql.select_iterator(
                database=database,
                columns={"name.is.here": Expression("n")},
                from_=sql.Expression("foo"),
                row_type=row_type,
            ):
                assert row["name.is.here"] >= 0
    finally:
        async with database.transaction():
            await database.execute(sql.Expression("DROP TABLE foo;"))


def test_param():
    assert sql.Param(10).type is int
    assert sql.Param("", str).type is str


async def test_find_pks(table):
    resource = sql.TableResource(table)
    keys = {uuid4() for _ in range(10)}
    async with table.database.transaction():
        for key in keys:
            await table.insert(DC(key=key))
    assert len(await resource.find_pks(keys)) == 10


async def test_str_literal():
    codec = sqlite.SQLiteCodec.get(Literal["a", "b", "c"])
    assert codec.sql_type == "TEXT"


async def test_int_literal():
    codec = sqlite.SQLiteCodec.get(Literal[1, 2, 3])
    assert codec.sql_type == "INTEGER"


async def test_mixed_literal():
    codec = sqlite.SQLiteCodec.get(Literal["a", 1, True])
    assert codec.sql_type == "TEXT"


async def test_select_page_iterator(database, table):
    Item = make_datacls("Item", fields=[("key", UUID), ("int_", int)])
    resource = sql.TableResource(table)
    async with database.transaction():
        for n in range(12):
            key = uuid4()
            await resource[key].put(DC(key=key, int_=n))
        rows = [
            row
            async for row in sql.select_iterator(
                database=database,
                columns={"key": "key", "int_": "int_"},
                from_="foo",
                row_type=Item,
            )
        ]
        assert len(rows) == 12


async def test_select_page_iterator_alias(database, table):
    Item = TypedDict("Item", {"key": UUID, "not-identifier": int})
    resource = sql.TableResource(table)
    async with database.transaction():
        for n in range(12):
            key = uuid4()
            await resource[key].put(DC(key=key, int_=n))
        rows = [
            row
            async for row in sql.select_iterator(
                database=database,
                columns={"key": "key", "not-identifier": "int_"},
                from_="foo",
                row_type=Item,
            )
        ]
        assert len(rows) == 12


async def test_select_page_synced(database, table):
    Item = make_datacls("Item", fields=[("key", UUID), ("int_plus_one", int)])
    resource = sql.TableResource(table)
    async with database.transaction():
        for n in range(12):
            key = uuid4()
            await resource[key].put(DC(key=key, int_=n))
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_plus_one": Expression("int_ + 1")},
            from_="foo",
            order_by=Expression("int_"),
            limit=5,
            item_type=Item,
        )
        assert len(page.items) == 5
        assert page.items[0].int_plus_one == 1
        assert page.items[-1].int_plus_one == 5
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_plus_one": Expression("int_ + 1")},
            from_="foo",
            order_by=Expression("int_"),
            limit=5,
            cursor=page.cursor,
            item_type=Item,
        )
        assert len(page.items) == 5
        assert page.items[0].int_plus_one == 6
        assert page.items[-1].int_plus_one == 10
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_plus_one": Expression("int_ + 1")},
            from_="foo",
            order_by=Expression("int_"),
            limit=50,
            cursor=page.cursor,
            item_type=Item,
        )
        assert len(page.items) == 2
        assert page.items[0].int_plus_one == 11
        assert page.items[-1].int_plus_one == 12
        assert page.cursor is None


async def test_select_page_broken_sync(database, table):
    Item = make_datacls("Item", fields=[("key", UUID), ("int_", int)])
    resource = sql.TableResource(table)
    async with database.transaction():
        for n in range(1, 13):
            key = uuid4()
            await resource[key].put(DC(key=key, int_=n))
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_": "int_"},
            from_="foo",
            order_by=Expression("int_"),
            limit=5,
            item_type=Item,
        )
        assert len(page.items) == 5
        assert page.items[0].int_ == 1
        assert page.items[-1].int_ == 5
        await table.delete(page.items[0].key)  # force pagination drift
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_": "int_"},
            from_="foo",
            order_by=Expression("int_"),
            limit=5,
            cursor=page.cursor,
            item_type=Item,
        )
        assert len(page.items) == 5
        assert page.items[0].int_ == 6
        assert page.items[-1].int_ == 10
        page = await sql.select_page(
            database=database,
            columns={"key": "key", "int_": "int_"},
            from_="foo",
            order_by=Expression("int_"),
            limit=50,
            cursor=page.cursor,
            item_type=Item,
        )
        assert len(page.items) == 2
        assert page.items[0].int_ == 11
        assert page.items[-1].int_ == 12
        assert page.cursor is None


async def test_patch_multi(database, table):
    keys1 = [uuid4() for _ in range(10)]
    keys2 = [uuid4() for _ in range(10)]
    resource = sql.TableResource(table)
    async with database.transaction():
        for key in keys1:
            await resource[key].put(DC(key=key, str_="a"))
        patch = [{"key": str(key), "str_": "b"} for key in keys1]
        await resource.patch(body=patch)
        for key in keys1:
            row = await resource[key].get()
            assert row.str_ == "b"
        patch = [{"key": str(key), "str_": "c"} for key in keys2]
        await resource.patch(body=patch)
        for key in keys2:
            row = await resource[key].get()
            assert row.str_ == "c"
        assert await resource.table.count() == 20
