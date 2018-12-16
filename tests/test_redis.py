import roax.resource as r
import roax.schema as s
import unittest

from redis import ConnectionPool, Redis
from roax.redis import RedisResource
from roax.resource import BadRequest, Conflict, NotFound, operation
from time import sleep
from uuid import uuid4


_schema = s.dict(
    properties = {
        "id": s.uuid(),
        "foo": s.str(),
        "bar": s.int(),
    },
    required = {"foo"},
)


class TestRedisResource(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pool = ConnectionPool()
        cls.r = Redis(connection_pool=pool)
        cls.r.flushdb()
        cls.rr = RedisResource(connection_pool=pool, schema=_schema, id_schema=_schema.properties["id"])

    @classmethod
    def tearDownClass(cls):
        cls.r.flushdb() 

    def test_create_conflict(self):
        id = uuid4()
        self.rr.create(id, {"id": id, "foo": "bar"})
        with self.assertRaises(Conflict):
            self.rr.create(id, {"id": id, "foo": "baz"})

    def test_read(self):
        id = uuid4()
        value = {"id": id, "foo": "bar"}
        response = self.rr.create(id, value)
        self.assertEqual(response["id"], id)
        self.assertEqual(self.rr.read(id), value)

    def test_read_notfound(self):
        id = uuid4()
        with self.assertRaises(NotFound):
            self.rr.read(id)

    def test_update(self):
        id = uuid4()
        value = {"id": id, "foo": "bar"}
        self.rr.create(id, value)
        value = {"id": id, "foo": "qux"}
        self.rr.update(id, value)
        self.assertEqual(self.rr.read(id), value)

    def test_update_notfound(self):
        id = uuid4()
        value = {"id": id, "foo": "bar"}
        with self.assertRaises(NotFound):
            self.rr.update(id, value)

    def test_delete_notfound(self):
        id = uuid4()
        with self.assertRaises(NotFound):
            self.rr.delete(id)

    def test_ttl(self):
        xr = RedisResource(connection_pool=ConnectionPool(),
                  schema=_schema, id_schema=_schema.properties["id"], ttl=.001)
        id = uuid4()
        value = {"id": id, "foo": "bar"}
        xr.create(id, value)
        xr.read(id)
        sleep(.002)
        with self.assertRaises(NotFound):
            read = xr.read(id)

if __name__ == "__main__":
    unittest.main()
