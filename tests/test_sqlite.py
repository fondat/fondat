import pytest

import fondat.error
import fondat.sql as sql
import fondat.sqlite as sqlite
import tempfile

from datetime import date, datetime
from fondat.data import datacls
from typing import Optional, TypedDict
from uuid import UUID, uuid4


pytestmark = pytest.mark.asyncio


@datacls
class DC:
    key: UUID
    str_: Optional[str]
    dict_: Optional[TypedDict("TD", {"a": int})]
    list_: Optional[list[int]]
    set_: Optional[set[str]]
    int_: Optional[int]
    float_: Optional[float]
    bool_: Optional[bool]
    bytes_: Optional[bytes]
    date_: Optional[date]
    datetime_: Optional[datetime]


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        yield database


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def table(database):
    foo = sql.Table("foo", database, DC, "key")
    await foo.create()
    yield foo
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
        await table.update(row)
        assert await table.read(row.key) == row
        await table.delete(row.key)
        assert await table.read(row.key) is None


async def test_binary(database):
    @datacls
    class Bin:
        key: UUID
        bin: bytes

    row = Bin(key=uuid4(), bin=b"\x01\x02\x03\x04\x05")
    table = sql.Table("bin", database, Bin, "key")
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
        for n in range(0, count):
            body = DC(
                key=uuid4(),
                str_=None,
                dict_=None,
                list_=None,
                set_=None,
                int_=None,
                float_=None,
                bool_=None,
                bytes_=None,
                date_=None,
                datetime_=None,
            )
            await table.insert(body)
        assert await table.count() == count
        async for result in await table.select(columns="key"):
            await table.delete(result["key"])
        assert await table.count() == 0


async def test_rollback(table):
    key = uuid4()
    try:
        async with table.database.transaction():
            row = DC(
                key=key,
                str_=None,
                dict_=None,
                list_=None,
                set_=None,
                int_=None,
                float_=None,
                bool_=None,
                bytes_=None,
                date_=None,
                datetime_=None,
            )
            await table.insert(row)
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    async with table.database.transaction():
        assert await table.read(key) is None


async def test_index(table):
    index = sql.Index("foo_ix_str", table, ("str_",))
    await index.create()
    await index.drop()


async def test_select_order(table):
    async with table.database.transaction():
        await table.insert(DC(key=uuid4(), str_="B", int_=1))
        await table.insert(DC(key=uuid4(), str_="B", int_=2))
        await table.insert(DC(key=uuid4(), str_="A"))
        await table.insert(DC(key=uuid4(), str_="C"))
        keys = [row["str_"] async for row in await table.select(order="str_")]
        assert keys == ["A", "B", "B", "C"]
        keys = [
            (row["str_"], row["int_"])
            async for row in await table.select(order=["str_", "int_"])
        ]
        assert keys == [("A", None), ("B", 1), ("B", 2), ("C", None)]
        keys = [
            (row["str_"], row["int_"]) async for row in await table.select(order="str_, int_")
        ]
        assert keys == [("A", None), ("B", 1), ("B", 2), ("C", None)]


async def test_resource_crud(table):
    pk = uuid4()
    resource = sql.table_resource(table, sql.row_resource(table))()[pk]
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
    await resource.put(row)
    assert await resource.get() == row
    await resource.delete()
    with pytest.raises(fondat.error.NotFoundError):
        await resource.get()
