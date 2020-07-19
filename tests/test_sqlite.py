import dataclasses
import pytest
import roax.resource as r
import roax.schema as s
import roax.sql as sql
import roax.sqlite as sqlite
import tempfile

from dataclasses import dataclass
from datetime import date, datetime
from uuid import uuid4


@dataclasses.dataclass
class DC:
    id: s.uuid()
    str: s.str(nullable=True)
    dict: s.dict({"a": s.int()}, nullable=True)
    list: s.list(s.int(), nullable=True)
    _set: s.set(s.str(), nullable=True)
    int: s.int(nullable=True)
    float: s.float(nullable=True)
    bool: s.bool(nullable=True)
    bytes: s.bytes(format="byte", nullable=True)
    date: s.date(nullable=True)
    datetime: s.datetime(nullable=True)


DC._schema = s.dataclass(DC)


class TR(sql.TableResource):
    def __init__(self, database):
        super().__init__(sql.Table("foo", DC._schema, "id"), database=database)


@pytest.fixture(scope="function")  # FIXME: scope to module with event_loop fixture?
async def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        foo = sql.Table("foo", DC._schema, "id")
        await database.create_table(foo)
        yield database
        await database.drop_table(foo)


@pytest.fixture(scope="function")
async def resource(database):
    stmt = sql.Statement()
    stmt.text("DELETE FROM FOO;")
    async with database.transaction() as t:
        await t.execute(stmt)
    resource = TR(database)
    return resource


pytestmark = pytest.mark.asyncio


async def test_binary(database):
    @dataclass
    class Bin:
        id: s.uuid()
        bin: s.bytes(format="binary")

    schema = s.dataclass(Bin)
    row = Bin(uuid4(), b"12345")
    table = sql.Table("bin", schema, "id")
    await database.create_table(table)
    try:
        resource = sql.TableResource(table, database)
        resource.database = database
        await resource.create(row.id, row)
        assert await resource.read(row.id) == row
        row.bin = b"bacon"
        await resource.update(row.id, row)
        assert (await resource.read(row.id)).bin == b"bacon"
    finally:
        await database.drop_table(table)


async def test_crud(resource):
    body = DC(
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
    await resource.create(body.id, body)
    assert await resource.read(body.id) == body
    body.dict = {"a": 2}
    body.list = [2, 3, 4]
    body._set = None
    body.int = 2
    body.float = 1.0
    body.bool = False
    body.bytes = None
    body.date = None
    body.datetime = None
    await resource.update(body.id, body)
    assert await resource.read(body.id) == body
    await resource.patch(body.id, {"str": "bacon"})
    body = await resource.read(body.id)
    assert body.str == "bacon"
    await resource.delete(body.id)
    with pytest.raises(r.NotFound):
        await resource.read(body.id)


async def testlist(resource):
    table = resource.table
    count = 10
    for n in range(0, count):
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
        assert await resource.create(id, body) == {"id": id}
    ids = await resource.list()
    assert len(ids) == count
    for id in ids:
        await resource.delete(id)
    assert len(await resource.list()) == 0


async def testlist_where(resource):
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
        assert await resource.create(id, body) == {"id": id}
    where = sql.Statement()
    where.text("int < ")
    where.param(10, table.columns["int"])
    ids = await resource.list(where=where)
    assert len(ids) == 10
    for id in await resource.list():
        await resource.delete(id)
    assert len(await resource.list()) == 0


async def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        await resource.delete(uuid4())


async def test_rollback(database, resource):
    table = resource.table
    assert len(await resource.list()) == 0
    try:
        async with database.transaction():  # transaction demarcation
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
            await resource.create(id, body)
            assert len(await resource.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(await resource.list()) == 0


"""
def test_schema_subclass_adapter(database):
    class strsub(s.str):
        pass

    adapter = database.adapter(strsub())
    assert adapter.sql_type == "TEXT"
"""