
import roax.resource as r
import roax.schema as s
import unittest

from datetime import timedelta
from roax.cache import cache
from roax.memory import MemoryResource
from roax.resource import operation, NotFound
from time import sleep


_schema = s.dict({
    "id": s.str(),
    "foo": s.str(),
})

class FooResource(MemoryResource):

    @operation(params={"id": _schema.properties["id"], "_body": _schema}, returns=s.dict({"id": _schema.properties["id"]}))
    def create(self, id, _body):
        return super().create(id, _body)

    @operation(params={"id": _schema.properties["id"]}, returns=_schema)
    def read(self, id):
        return {**super().read(id), "id": id}

    @operation(params={"id": _schema.properties["id"], "_body": _schema})
    def update(self, id, _body):
        return super().update(id, _body)

    @operation(params={"id": _schema.properties["id"]})
    def delete(self, id):
        return super().delete(id)

    @operation(type="query", returns=s.list(_schema.properties["id"]))
    def list(self):
        return super().list()


class TestCache(unittest.TestCase):

    @cache(max_size=1000)
    class SimpleCacheResource(FooResource): pass

    @cache(max_age=timedelta(microseconds=1000))
    class QuickCacheResource(FooResource): pass

    @cache(max_size=1)
    class SmallCacheResource(FooResource): pass

    def test_missing_cache_params(self):
        with self.assertRaises(ValueError):
            @cache()
            class BadCacheResource(FooResource): pass

    def test_read_cache(self):
        cr = self.SimpleCacheResource()
        _id = "1"
        value = {"id": _id, "foo": "bar"}
        cr.create(_id, value)
        with self.assertRaises(KeyError):
            cr._roax_cache_["1"]
        read = cr.read("1")
        self.assertEqual(read, value)
        self.assertEqual(cr._roax_cache_["1"][1], value)

    def test_delete_cache(self):
        cr = self.SimpleCacheResource()
        cr.create("1", {"foo": "bar"})
        self.assertEqual(len(cr._roax_cache_), 0)
        cr.read("1")
        cr.delete("1")
        self.assertEqual(len(cr._roax_cache_), 0)
        with self.assertRaises(NotFound):
            cr.read("1")

    def test_update_cache(self):
        cr = self.SimpleCacheResource()
        self.assertEqual(len(cr._roax_cache_), 0)
        cr.create("1", {"foo": "bar"})
        self.assertEqual(len(cr._roax_cache_), 0)
        cr.read("1")
        self.assertEqual(len(cr._roax_cache_), 1)
        cr.update("1", {"foo": "qux"})
        self.assertEqual(len(cr._roax_cache_), 0)
        self.assertEqual(cr.read("1")["foo"], "qux")

    def test_expiry(self):
        cr = self.QuickCacheResource()
        cr.create("1", {"foo": "bar"})
        read = cr.read("1")
        del cr.mapping["1"]
        self.assertEqual(read, cr.read("1"))  # fetches from cache
        sleep(0.001)
        with self.assertRaises(NotFound):
            read = cr.read("1")

    def test_eviction(self):
        cr = self.SmallCacheResource()
        cr.create("1", {"foo": "bar"})
        cr.create("2", {"foo": "qux"})
        cr.read("1")
        self.assertEqual(len(cr._roax_cache_), 1)
        value = cr.read("2")
        self.assertEqual(len(cr._roax_cache_), 1)
        self.assertEqual(cr._roax_cache_["2"][1], value)

    def test_invalidate_single(self):
        cr = self.SimpleCacheResource()
        cr.create("1", {"foo": "bar"})
        cr.create("2", {"foo": "qux"})
        cr.read("2")
        cr.read("1")
        self.assertEqual(len(cr._roax_cache_), 2)
        cr._invalidate("2")
        self.assertEqual(len(cr._roax_cache_), 1)
        self.assertEqual(cr._roax_cache_["1"][1]["foo"], "bar")

    def test_invalidate_all(self):
        cr = self.SimpleCacheResource()
        cr.create("1", {"foo": "bar"})
        cr.create("2", {"foo": "qux"})
        cr.read("1")
        cr.read("2")
        self.assertEqual(len(cr._roax_cache_), 2)
        cr._invalidate()
        self.assertEqual(len(cr._roax_cache_), 0)


if __name__ == "__main__":
    unittest.main()
