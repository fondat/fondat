import dataclasses
import pytest
import fondat.resource as r
import fondat.schema as s
import fondat.sql as sql
import fondat.sqlite as sqlite
import tempfile

from dataclasses import dataclass
from datetime import date, datetime
from uuid import uuid4


pytestmark = pytest.mark.asyncio


@s.data
class DC:
    id: s.uuid()
    str: s.str(nullable=True)
    dict: s.dict({"a": s.int()}, nullable=True)
    list: s.list(s.int(), nullable=True)
    _set: s.set(s.str(), nullable=True)
    int: s.int(nullable=True)
    float: s.float(nullable=True)
    bool: s.bool(nullable=True)
    bytes: s.bytes(nullable=True)
    date: s.date(nullable=True)
    datetime: s.datetime(nullable=True)


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        yield database


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def table(database):
    foo = sql.Table(database, "foo", DC._schema, "id")
    await foo.create()
    yield foo
    await foo.drop()


@pytest.fixture(scope="function")
async def resource(table):
    stmt = sql.Statement()
    stmt.text("DELETE FROM FOO;")
    async with table.database.transaction() as t:
        await t.execute(stmt)
    resource = sql.table_resource(table)
    return resource()


async def test_gpppd(resource):
    data = DC(
        id=uuid4(),
        str="string",
        dict={"a": 1},
        list=[1, 2, 3],
        _set={"foo", "bar"},
        int=1,
        float=2.3,
        bool=True,
        bytes=b"12345",
        date=s.date().str_decode("2019-01-01"),
        datetime=s.datetime().str_decode("2019-01-01T01:01:01Z"),
    )
    await resource.post(data.id, data)
    assert await resource.get(data.id) == data
    data.dict = {"a": 2}
    data.list = [2, 3, 4]
    data._set = None
    data.int = 2
    data.float = 1.0
    data.bool = False
    data.bytes = None
    data.date = None
    data.datetime = None
    await resource.put(data.id, data)
    assert await resource.get(data.id) == data
    await resource.patch(data.id, {"str": "bacon"})
    data = await resource.get(data.id)
    assert data.str == "bacon"
    await resource.delete(data.id)
    with pytest.raises(r.NotFound):
        await resource.get(data.id)


async def test_binary(database):
    @dataclass
    class Bin:
        id: s.uuid()
        bin: s.bytes(format="binary")

    schema = s.dataclass(Bin)
    row = Bin(uuid4(), b"\x01\x02\x03\x04\x05")
    table = sql.Table(database, "bin", schema, "id")
    await table.create()
    try:
        resource = sql.table_resource(table)()
        await resource.post(row.id, row)
        assert await resource.get(row.id) == row
        row.bin = b"bacon"
        await resource.put(row.id, row)
        assert (await resource.get(row.id)).bin == b"bacon"
    finally:
        await table.drop()


async def test_list(resource):
    table = resource.table
    count = 10
    for n in range(0, count):
        id = uuid4()
        data = DC(
            id=id,
            str=None,
            dict=None,
            list=None,
            _set=None,
            int=None,
            float=None,
            bool=None,
            bytes=None,
            date=None,
            datetime=None,
        )
        assert await resource.post(id, data) == {"id": id}
    ids = await table.list()
    assert len(ids) == count
    for id in ids:
        await resource.delete(id)
    assert len(await table.list()) == 0


async def test_list_where(resource):
    table = resource.table
    for n in range(0, 20):
        id = uuid4()
        body = DC(
            id=id,
            str=None,
            dict=None,
            list=None,
            _set=None,
            int=n,
            float=None,
            bool=None,
            bytes=None,
            date=None,
            datetime=None,
        )
        assert await resource.post(id, body) == {"id": id}
    where = sql.Statement()
    where.text("int < ")
    where.param(10, table.columns["int"])
    ids = await table.list(where=where)
    assert len(ids) == 10
    for id in await table.list():
        await resource.delete(id)
    assert len(await table.list()) == 0


async def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        await resource.delete(uuid4())


async def test_rollback(resource):
    table = resource.table
    assert len(await table.list()) == 0
    try:
        async with table.database.transaction():  # transaction demarcation
            id = uuid4()
            body = DC(
                id=id,
                str=None,
                dict=None,
                list=None,
                _set=None,
                int=None,
                float=None,
                bool=None,
                bytes=None,
                date=None,
                datetime=None,
            )
            await resource.post(id, body)
            assert len(await table.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(await table.list()) == 0


def test_schema_subclass_adapter(database):
    class strsub(s.str):
        pass

    adapter = database.adapters[strsub()]
    assert adapter.sql_type == "TEXT"
