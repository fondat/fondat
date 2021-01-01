import dataclasses
import enum
import pytest
import fondat.sql as sql
import fondat.sqlite as sqlite
import tempfile

from dataclasses import dataclass
from datetime import date, datetime
from typing import Annotated, Optional, TypedDict
from uuid import UUID, uuid4


pytestmark = pytest.mark.asyncio


class StrEnum(enum.Enum):
    A = "a"
    B = "b"
    C = "c"


class IntEnum(enum.Enum):
    ONE = 1
    TWO = 2
    THREE = 3


@dataclass
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
    str_enum_: Optional[StrEnum]
    int_enum_: Optional[IntEnum]


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        yield database


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def table(database):
    foo = sql.Table("foo", database, DC, "key")
    async with database.transaction():
        await foo.create()
    yield foo
    async with database.transaction():
        await foo.drop()


async def test_crud(table):
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
        str_enum_=StrEnum.A,
        int_enum_=IntEnum.ONE,
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
        row.str_enum_ = StrEnum.B
        row.int_enum_ = IntEnum.TWO
        await table.update(row)
        assert await table.read(row.key) == row
        await table.delete(row.key)
        assert await table.read(row.key) is None


async def test_binary(database):
    @dataclass
    class Bin:
        key: UUID
        bin: bytes

    row = Bin(uuid4(), b"\x01\x02\x03\x04\x05")
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
                str_enum_=None,
                int_enum_=None,
            )
            await table.insert(body)
        assert await table.count() == count
        async for result in await table.select("key"):
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
                str_enum_=None,
                int_enum_=None,
            )
            await table.insert(row)
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    async with table.database.transaction():
        assert await table.read(key) is None
