import dataclasses
import pytest
import roax.db as db
import roax.resource as r
import roax.schema as s
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


class TR(db.TableResource):
    def __init__(self, database):
        super().__init__(db.Table("foo", DC._schema, "id"), database=database)


@pytest.fixture(scope="module")
def database():
    with tempfile.TemporaryDirectory() as dir:
        database = sqlite.Database(f"{dir}/test.db")
        foo = db.Table("foo", DC._schema, "id")
        database.create_table(foo)
        yield database
        database.drop_table(foo)


@pytest.fixture(scope="function")
def resource(database):
    with database.cursor() as cursor:
        cursor.execute("DELETE FROM FOO;")
    resource = TR(database)
    return resource


def test_binary(database):
    @dataclass
    class Bin:
        id: s.uuid()
        bin: s.bytes(format="binary")

    schema = s.dataclass(Bin)
    row = Bin(uuid4(), b"12345")
    table = db.Table("bin", schema, "id")
    database.create_table(table)
    try:
        resource = db.TableResource(table)
        resource.database = database
        resource.create(row.id, row)
        assert resource.read(row.id) == row
        row.bin = b"bacon"
        resource.update(row.id, row)
        assert resource.read(row.id).bin == b"bacon"
    finally:
        database.drop_table(table)


def test_crud(resource):
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
    resource.create(body.id, body)
    assert resource.read(body.id) == body
    body.dict = {"a": 2}
    body.list = [2, 3, 4]
    body._set = None
    body.int = 2
    body.float = 1.0
    body.bool = False
    body.bytes = None
    body.date = None
    body.datetime = None
    resource.update(body.id, body)
    assert resource.read(body.id) == body
    resource.patch(body.id, {"str": "bacon"})
    body = resource.read(body.id)
    assert body.str == "bacon"
    resource.delete(body.id)
    with pytest.raises(r.NotFound):
        resource.read(body.id)


def testlist(resource):
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
        assert resource.create(id, body) == {"id": id}
    ids = resource.list()
    assert len(ids) == count
    for id in ids:
        resource.delete(id)
    assert len(resource.list()) == 0


def testlist_where(resource):
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
        assert resource.create(id, body) == {"id": id}
    where = resource.database.query()
    where.text("int < ")
    where.value(table.columns["int"], 10)
    ids = resource.list(where=where)
    assert len(ids) == 10
    for id in resource.list():
        resource.delete(id)
    assert len(resource.list()) == 0


def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        resource.delete(uuid4())


def test_rollback(database, resource):
    table = resource.table
    assert len(resource.list()) == 0
    try:
        with database.connect():  # transaction demarcation
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
            resource.create(id, body)
            assert len(resource.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(resource.list()) == 0


def test_schema_subclass_adapter(database):
    class strsub(s.str):
        pass

    adapter = database.adapter(strsub())
    assert adapter.sql_type == "TEXT"
