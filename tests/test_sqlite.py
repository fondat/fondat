import pytest
import roax.schema as s
import roax.db as db
import roax.sqlite as sqlite

from datetime import date, datetime
from roax.resource import NotFound
from uuid import uuid4


create_table = """
CREATE TABLE FOO (
    _id TEXT,
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

_schema = s.dict(
    {
        "_id": s.uuid(),
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
    db = sqlite.Database("/tmp/foo.db")
    with db.cursor() as cursor:
        cursor.execute(create_table)
    return db


@pytest.fixture(scope="module")
def table():
    return sqlite.Table("foo", _schema, "_id")


@pytest.fixture(scope="module")
def resource(database, table):
    return db.TableResource(database, table)


def test_resource_crud(resource):
    body = {
        "_id": uuid4(),
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
    with pytest.raises(NotFound):
        resource.read(body["_id"])
