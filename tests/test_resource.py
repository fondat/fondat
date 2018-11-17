import roax.schema as s
import unittest
import uuid

from roax.resource import Resource, Resources, operation


body_schema = s.dict({
    "id": s.uuid(required=False),
    "foo": s.str()
})


class R1(Resource):

    schema = body_schema
    
    @operation(params={"body": body_schema}, returns=s.uuid())
    def create(self, body):
        return uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9")

    @operation(type="query", returns=s.str())
    def foo(self):
        return "bar"

class R2(Resource):
    
    schema = body_schema
    
    def __init__(self):
        super().__init__()
        self.create = operation(params={"body": body_schema}, returns=s.uuid())(self.create)

    def create(self, body):
        return uuid.UUID("f5808e7e-09c0-4f0c-ae6f-a9b30bd23290")


class InvalidOperationTypeResource(Resource):

    def __init__(self):
        super().__init__()
        self.not_a_valid_operation_type = operation()(self.not_a_valid_operation_type)    

    def not_a_valid_operation_type(self):
        pass

class TestResource(unittest.TestCase):

    def test_call(self):
        result = R1().operations["create"].call(body={"foo":"bar"})

    def test_init_wrap(self):
        result = R2().operations["create"].call(body={"foo":"bar"})

    def test_invalid_operation_type(self):
        with self.assertRaises(ValueError):
            InvalidOperationTypeResource()

    def test_override_operation_type(self):
        class OverrideOperationTypeResource(Resource):
            @operation(type="create")
            def not_a_valid_operation_type(self):
                pass


class TestResources(unittest.TestCase):

    def test_resources(self):
        mod = R1.__module__
        resources = Resources({
            "r1": "{}.R1".format(mod),
            "r2": "{}.R2".format(mod),
        })
        r1 = resources.r1
        self.assertEqual(r1.foo(), "bar")


if __name__ == "__main__":
    unittest.main()
