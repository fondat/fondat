
import roax.resource as r
import roax.schema as s
import unittest

from roax.file import FileResource
from roax.resource import InternalServerError, NotFound, operation
from tempfile import TemporaryDirectory
from uuid import uuid4


class TestFileResource(unittest.TestCase):

    def test_crud_dict(self):
        
        _schema = s.dict({
            "id": s.uuid(required=False),
            "foo": s.str(),
            "bar": s.int(),
        })

        class FooResource(FileResource):

            schema = _schema
            id_schema = _schema.properties["id"]

            @operation(params={"_body": _schema}, returns=s.dict({"id": _schema.properties["id"]}))
            def create(self, _body):
                return super().create(uuid4(), _body)

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

        with TemporaryDirectory() as dir:
            rs = FooResource(dir)
            r1 = { "foo": "hello", "bar": 1 }
            id = rs.create(r1)["id"]
            r1["id"] = id
            r2 = rs.read(id)
            self.assertEqual(r1, r2) 
            r1["bar"] = 2
            rs.update(id, r1)
            r2 = rs.read(id)
            self.assertEqual(r1, r2) 
            rs.delete(id)
            self.assertEqual(rs.list(), [])

    
    def test_crud_str(self):

        _schema = s.str()

        class FRS(FileResource):

            schema = _schema

            @operation(params={"id": s.str(), "_body": _schema}, returns=s.dict({"id": s.str()}))
            def create(self, id, _body):
                return super().create(id, _body)

            @operation(params={"id": s.str()}, returns=_schema)
            def read(self, id):
                return super().read(id)

            @operation(params={"id": s.str(), "_body": _schema})
            def update(self, id, _body):
                return super().update(id, _body)

            @operation(params={"id": s.str()})
            def delete(self, id):
                return super().delete(id)

            @operation(type="query", returns=s.list(s.str()))
            def list(self):
                return super().list()

        with TemporaryDirectory() as dir:
            frs = FRS(dir, extension=".txt")
            body = "你好，世界!"
            id = "hello_world"
            self.assertEqual(id, frs.create(id, body)["id"])
            self.assertEqual(frs.list(), [id])
            self.assertEqual(body, frs.read(id))
            body = "Goodbye world!"
            frs.update(id, body)
            self.assertEqual(body, frs.read(id))
            frs.delete(id)
            self.assertEqual(frs.list(), [])

    def test_crud_bytes(self):
        with TemporaryDirectory() as dir:
            frs = FileResource(dir, schema=s.bytes(), extension=".bin")
            body = b"\x00\x0e\0x01\0x01\0x00"
            id = "binary"
            self.assertEqual(id, frs.create(id, body)["id"])
            self.assertEqual(frs.list(), [id])
            self.assertEqual(body, frs.read(id))
            body = bytes((1,2,3,4,5))
            frs.update(id, body)
            self.assertEqual(body, frs.read(id))
            frs.delete(id)
            self.assertEqual(frs.list(), [])

    def test_quote_unquote(self):
        with TemporaryDirectory() as dir:
            fr = FileResource(dir, schema=s.bytes(), extension=".bin")
            body = b"body"
            id = "resource%identifier"
            self.assertEqual(id, fr.create(id, body)["id"])
            fr.delete(id)

    def test_invalid_directory(self):
        with TemporaryDirectory() as dir:
            fr = FileResource(dir, schema=s.bytes(), extension=".bin")
        # directory should now be deleted underneath the resource
        body = b"body"
        id = "resource%identifier"
        with self.assertRaises(InternalServerError):
            fr.create(id, body)
        with self.assertRaises(NotFound):
            fr.read(id)
        with self.assertRaises(NotFound):
            fr.update(id, body)
        with self.assertRaises(NotFound):
            fr.delete(id)
        with self.assertRaises(InternalServerError):
            fr.list()


if __name__ == "__main__":
    unittest.main()
