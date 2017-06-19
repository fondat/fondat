
import roax.schema as s
import unittest

from roax.file import FileResourceSet
from tempfile import TemporaryDirectory
from uuid import uuid4

class FooResourceSet(FileResourceSet):

    schema = s.dict({
        "_id": s.uuid(required=False),
        "foo": s.str(),
        "bar": s.int()
    })

    def __init__(self, dir):
        super().__init__(dir)

    def gen_id(self):
        return uuid4()

class TestFileResource(unittest.TestCase):

    def _equal(self, fn, val):
        self.assertEqual(val, fn(val))

    def _error(self, fn, val):
        with self.assertRaises(s.SchemaError):
            fn(val)

    def test_crud(self):
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

if __name__ == "__main__":
    unittest.main()
