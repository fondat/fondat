
import roax.schema as s
import unittest
import uuid

from roax.resource import ResourceSet, method

_doc_schema = s.dict({
    "_id": s.uuid(required=False),
    "foo": s.str()
})

class R1(ResourceSet):

    schema = _doc_schema
    
    @method(params=s.dict({"_doc": _doc_schema}), returns=s.dict({"_id": s.uuid()}))
    def create(self, _doc):
        return { "_id": uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9") }

class R2(ResourceSet):
    
    schema = _doc_schema
    
    def __init__(self):
        super().__init__()
        self.create = method(params=s.dict({"_doc": _doc_schema}), returns=s.dict({"_id": s.uuid()}))(self.create)
    def create(self, _doc):
        return { "_id": uuid.UUID("f5808e7e-09c0-4f0c-ae6f-a9b30bd23290") }

class TestResourceSet(unittest.TestCase):

    def test_call(self):
        result = R1().call("create", params={"_doc": {"foo": "bar"}})

    def test_init_wrap(self):
        result = R2().call("create", params={"_doc": {"foo": "bar"}})

if __name__ == "__main__":
    unittest.main()
