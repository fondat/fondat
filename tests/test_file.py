
import roax.resource as r
import roax.schema as s
import unittest

from roax.file import FileResourceSet
from tempfile import TemporaryDirectory
from uuid import uuid4

class TestFileResource(unittest.TestCase):

    def test_crud(self):
        
        class FooResourceSet(FileResourceSet):

            schema = s.dict({
                "_id": s.uuid(required=False),
                "foo": s.str(),
                "bar": s.int()
            })

            def __init__(self, dir):
                super().__init__(dir)

            def gen_id(self, _doc):
                return uuid4()

        with TemporaryDirectory() as dir:
            rs = FooResourceSet(dir)
            r1 = { "foo": "hello", "bar": 1 }
            _id = rs.create(r1)["_id"]
            r1["_id"] = _id
            r2 = rs.read(_id)
            self.assertEqual(r1, r2) 
            r1["bar"] = 2
            rs.update(_id, r1)
            r2 = rs.read(_id)
            self.assertEqual(r1, r2) 
            rs.delete(_id)
            self.assertEqual(rs.query_ids(), [])

    def test_rev(self):

        class FooResourceSet(FileResourceSet):

            schema = s.dict({
                "_id": s.str(required=False),
                "_rev": s.int(required=False),
                "foo": s.str()
            })

            def __init__(self, dir, mkdir=False):
                super().__init__(dir, mkdir=mkdir)

            def gen_id(self, _doc):
                return str(uuid4())

            def gen_rev(self, old, new):
                return old["_rev"] + 1 if old else 1

        with TemporaryDirectory() as dir:
            rs = FooResourceSet(dir + "/resources/foo", mkdir=True)
            _doc = { "foo": "bar" }
            _id = "/\\:*?\"<>|"
            result = rs.create(_doc, _id) # should be URL-encoded
            self.assertEqual(_id, result["_id"])
            self.assertEqual(rs.query_ids(), [_id])
            _rev = result["_rev"]
            self.assertEqual(_rev, 1)
            _doc = { "foo": "qux" }
            result = rs.update(_id, _doc, 1)
            self.assertEqual(result["_rev"], 2)
            with self.assertRaises(r.PreconditionFailed):
                rs.update(_id, _doc, 1)
            _doc = { "foo": "bar" }
            result = rs.update(_id, _doc)
            self.assertEqual(result["_rev"], 3)

    def test_lamda(self):

        schema = s.dict({
            "_id": s.uuid(required=False),
            "_rev": s.int(required=False),
            "foo": s.str()
        })

        with TemporaryDirectory() as dir:
            rs = FileResourceSet(
                dir + "/resources/foo",
                schema=schema,
                gen_id=lambda _doc: uuid4(),
                gen_rev=lambda old, new: old["_rev"] + 1 if old else 1
            )
            _doc = { "foo": "bar" }
            result = rs.create(_doc)
            _id = result["_id"]
            _rev = result["_rev"]
            self.assertEqual(_rev, 1)
            _doc = { "foo": "qux" }
            result = rs.update(_id, _doc, _rev)
            self.assertEqual(result["_rev"], 2)

if __name__ == "__main__":
    unittest.main()
