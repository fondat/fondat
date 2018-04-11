
import roax.resource as r
import roax.schema as s
import unittest

from roax.file import FileResource
from tempfile import TemporaryDirectory
from uuid import uuid4


class TestFileResource(unittest.TestCase):

    def test_crud_dict(self):
        
        class FooResource(FileResource):

            schema = s.dict({
                "id": s.uuid(required=False),
                "foo": s.str(),
                "bar": s.int(),
            })

            def create(self, _body):
                return super().create(_body, uuid4())

        with TemporaryDirectory() as dir:
            rs = FooResource(dir)
            r1 = { "foo": "hello", "bar": 1 }
            id = rs.create(r1)
            r1["id"] = id
            r2 = rs.read(id)
            self.assertEqual(r1, r2) 
            r1["bar"] = 2
            rs.update(id, r1)
            r2 = rs.read(id)
            self.assertEqual(r1, r2) 
            rs.delete(id)
            self.assertEqual(rs.query_ids(), [])

    def test_crud_str(self):

        class FRS(FileResource):

            schema = s.str()

        with TemporaryDirectory() as dir:
            frs = FRS(dir, extension=".txt")
            body = "你好，世界!"
            id = "hello_world"
            self.assertEqual(id, frs.create(body, id))
            self.assertEqual(frs.query_ids(), [id])
            self.assertEqual(body, frs.read(id))
            body = "Goodbye world!"
            frs.update(id, body)
            self.assertEqual(body, frs.read(id))
            frs.delete(id)
            self.assertEqual(frs.query_ids(), [])

    def test_crud_bytes(self):

        with TemporaryDirectory() as dir:
            frs = FileResource(dir, schema=s.bytes(), extension=".bin")
            body = b"\x00\x0e\0x01\0x01\0x00"
            id = "binary"
            self.assertEqual(id, frs.create(body, id))
            self.assertEqual(frs.query_ids(), [id])
            self.assertEqual(body, frs.read(id))
            body = bytes((1,2,3,4,5))
            frs.update(id, body)
            self.assertEqual(body, frs.read(id))
            frs.delete(id)
            self.assertEqual(frs.query_ids(), [])


if __name__ == "__main__":
    unittest.main()
