
import roax.schema as s
import unittest
import uuid

from roax.resource import Resource, method

_doc_schema = s.dict(
    fields = {
        "_id": s.uuid(required=False),
        "foo": s.str()
    }
)

class R1(Resource):
    
    @method(params=s.dict(fields={"_doc": _doc_schema}), returns=s.dict(fields={"_id": s.uuid()}))
    def create(self, _doc):
        return { "_id": uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9") }

class R2(Resource):
    def __init__(self):
        super().__init__()
        self.register_method(self.create, params=s.dict(fields={"_doc": _doc_schema}), returns=s.dict(fields={"_id": s.uuid()}))
    def create(self, _doc):
        return { "_id": uuid.UUID("f5808e7e-09c0-4f0c-ae6f-a9b30bd23290") }

class TestResource(unittest.TestCase):

    def test_call(self):
        result = R1().call_method("create", params={"_doc": {"foo": "bar"}})

    def test_init_wrap(self):
        result = R2().call_method("create", params={"_doc": {"foo": "bar"}})

if __name__ == "__main__":
    unittest.main()
