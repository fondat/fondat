
import roax.resource as r
import roax.schema as s
import unittest

from roax.file import FileResourceSet
from tempfile import TemporaryDirectory
from uuid import uuid4

class TestFileResource(unittest.TestCase):

    def test_crud_dict(self):
        
        class FooResourceSet(FileResourceSet):

            schema = s.dict({
                "_id": s.uuid(required=False),
                "foo": s.str(),
                "bar": s.int(),
            })

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

    def test_crud_str(self):

        class FRS(FileResourceSet):

            schema = s.str()

            def create(self, _doc, _id): # make _id required
                return super().create(_doc, _id)

        with TemporaryDirectory() as dir:
            frs = FRS(dir, extension=".txt")
            _doc = "你好，世界!"
            _id = "hello_world"
            self.assertEqual(_id, frs.create(_doc, _id)["_id"])
            self.assertEqual(frs.query_ids(), [_id])
            self.assertEqual(_doc, frs.read(_id))
            _doc = "Goodbye world!"
            frs.update(_id, _doc)
            self.assertEqual(_doc, frs.read(_id))
            frs.delete(_id)
            self.assertEqual(frs.query_ids(), [])

    def test_crud_bytes(self):

        with TemporaryDirectory() as dir:
            frs = FileResourceSet(dir, schema=s.bytes(), extension=".bin")
            _doc = b"\x00\x0e\0x01\0x01\0x00"
            _id = "binary"
            self.assertEqual(_id, frs.create(_doc, _id)["_id"])
            self.assertEqual(frs.query_ids(), [_id])
            self.assertEqual(_doc, frs.read(_id))
            _doc = bytes((1,2,3,4,5))
            frs.update(_id, _doc)
            self.assertEqual(_doc, frs.read(_id))
            frs.delete(_id)
            self.assertEqual(frs.query_ids(), [])

    def test_lambda(self):

        schema = s.dict({
            "_id": s.uuid(required=False),
            "foo": s.str(),
        })

        with TemporaryDirectory() as dir:
            rs = FileResourceSet(
                dir + "/resources/foo",
                schema=schema,
                gen_id=lambda _doc: uuid4(),
            )
            _doc = { "foo": "bar" }
            result = rs.create(_doc)
            _id = result["_id"]
            _doc = { "foo": "qux" }
            result = rs.update(_id, _doc)

if __name__ == "__main__":
    unittest.main()
