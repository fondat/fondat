import roax.schema as s
import unittest
import uuid

from roax.resource import Resource, method

body_schema = s.dict({
    "id": s.uuid(required=False),
    "foo": s.str()
})

class R1(Resource):

    schema = body_schema
    
    @method(params={"body": body_schema}, returns=s.uuid())
    def create(self, body):
        return uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9")

class R2(Resource):
    
    schema = body_schema
    
    def __init__(self):
        super().__init__()
        self.create = method(params={"body": body_schema}, returns=s.uuid())(self.create)

    def create(self, body):
        return uuid.UUID("f5808e7e-09c0-4f0c-ae6f-a9b30bd23290")

class TestResource(unittest.TestCase):

    def test_call(self):
        result = R1().call("create", params={"body": {"foo": "bar"}})

    def test_init_wrap(self):
        result = R2().call("create", params={"body": {"foo": "bar"}})

if __name__ == "__main__":
    unittest.main()
