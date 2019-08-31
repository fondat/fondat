import pytest
import roax.db as db
import roax.resource as r
import roax.schema as s
import roax.sqlite as sqlite
import tempfile

from datetime import date, datetime
from uuid import uuid4


_schema = s.dict(
    {
        "_id": s.uuid(),
        "_str": s.str(),
        "_dict": s.dict({"a": s.int()}),
        "_list": s.list(s.int()),
        "_set": s.set(s.str()),
        "_int": s.int(),
        "_float": s.float(),
        "_bool": s.bool(),
        "_bytes": s.bytes(format="binary"),
        "_date": s.date(),
        "_datetime": s.datetime(),
    }
)


@pytest.fixture(scope="module")
def database():
    with tempfile.TemporaryDirectory() as dir:
        db = sqlite.Database(f"{dir}/test.db")
        with db.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE FOO (
                    _id TEXT,
                    _str TEXT,
                    _dict TEXT,
                    _list TEXT,
                    _set TEXT,
                    _int INT,
                    _float REAL,
                    _bool INT,
                    _bytes BLOB,
                    _date TEXT,
                    _datetime TEXT
                );
            """
            )
        yield db
        with db.cursor() as cursor:
            cursor.execute("DROP TABLE FOO;")


@pytest.fixture(scope="module")
def table(database):
    return sqlite.Table(database, "foo", _schema, "_id")


@pytest.fixture(scope="function")
def resource(table):
    with table.cursor() as cursor:
        cursor.execute("DELETE FROM FOO;")
    return db.TableResource(table)


def test_crud(resource):
    body = {
        "_id": uuid4(),
        "_str": "string",
        "_dict": {"a": 1},
        "_list": [1, 2, 3],
        "_set": {"foo", "bar"},
        "_int": 1,
        "_float": 2.3,
        "_bool": True,
        "_bytes": b"12345",
        "_date": s.date().str_decode("2019-01-01"),
        "_datetime": s.datetime().str_decode("2019-01-01T01:01:01Z"),
    }
    resource.create(body["_id"], body)
    assert resource.read(body["_id"]) == body
    body["_dict"] = {"a": 2}
    body["_list"] = [2, 3, 4]
    del body["_set"]
    body["_int"] = 2
    body["_float"] = 1.0
    body["_bool"] = False
    del body["_bytes"]
    del body["_date"]
    del body["_datetime"]
    resource.update(body["_id"], body)
    assert resource.read(body["_id"]) == body
    resource.delete(body["_id"])
    with pytest.raises(r.NotFound):
        resource.read(body["_id"])


def test_list(resource):
    table = resource.table
    count = 10
    for n in range(0, count):
        id = uuid4()
        assert resource.create(id, {"_id": id}) == {"id": id}
    ids = table.list()
    assert len(ids) == count
    for id in ids:
        resource.delete(id)
    assert len(table.list()) == 0


def test_list_where(resource):
    table = resource.table
    for n in range(0, 20):
        id = uuid4()
        assert resource.create(id, {"_id": id, "_int": n}) == {"id": id}
    where = table.query()
    where.text("_int < ")
    where.value("_int", 10)
    ids = table.list(where=where)
    print(f"ids = {ids}")
    assert len(ids) == 10
    for id in table.list():
        resource.delete(id)
    assert len(table.list()) == 0


def test_delete_NotFound(resource):
    with pytest.raises(r.NotFound):
        resource.delete(uuid4())


def test_rollback(database, resource):
    table = resource.table
    assert len(table.list()) == 0
    try:
        with database.connect():  # transaction demarcation
            id = uuid4()
            resource.create(id, {"_id": id})
            assert len(table.list()) == 1
            raise RuntimeError  # force rollback
    except RuntimeError:
        pass
    assert len(table.list()) == 0


class CustomType:
    def __init__(self, value):
        self.value = value

    def __eq__(self, value):
        return self.value == value


class CustomTypeSchema(s.type):
    def __init__(self, **kwargs):
        super().__init__(python_type=CustomType, **kwargs)

    def json_encode(self, value):
        return self.str_encode(value)

    def json_decode(self, value):
        return self.str_decode(value)

    def str_encode(self, value):
        self.validate(value)
        return s.int().str_encode(value.value)

    def str_decode(self, value):
        result = CustomType(s.int().str_decode(value))
        self.validate(result)
        return result

    def validate(self, value):
        return s.int().validate(value.value)


def test_custom_adapter(database):
    schema = s.dict({"id": s.str(), "custom": CustomTypeSchema()})
    table = db.Table(database, "custom", schema, "id", {})
    assert table.adapter("custom").encode(schema["custom"], CustomType(123)) == "123"
    assert table.adapter("custom").decode(schema["custom"], "456") == CustomType(456)
