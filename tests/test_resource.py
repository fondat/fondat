import pytest
import roax.schema as s
import uuid

from roax.resource import Resource, Resources, operation


body_schema = s.dict(properties={"id": s.uuid(), "foo": s.str()}, required={"foo"})


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
        self.create = operation(params={"body": body_schema}, returns=s.uuid())(
            self.create
        )

    def create(self, body):
        return uuid.UUID("f5808e7e-09c0-4f0c-ae6f-a9b30bd23290")


class R3(Resource):
    @operation(type="query", returns=s.str())
    def qux(self):
        return "baz"


class InvalidOperationTypeResource(Resource):
    def __init__(self):
        super().__init__()
        self.not_a_valid_operation_type = operation()(self.not_a_valid_operation_type)

    def not_a_valid_operation_type(self):
        pass


def test_call():
    result = R1().operations["create"].call(body={"foo": "bar"})


def test_init_wrap():
    result = R2().operations["create"].call(body={"foo": "bar"})


def test_invalid_operation_type():
    with pytest.raises(ValueError):
        InvalidOperationTypeResource()


def test_override_operation_type():
    class OverrideOperationTypeResource(Resource):
        @operation(type="create")
        def not_a_valid_operation_type(self):
            pass


def test_resources():
    mod = R1.__module__
    resources = Resources({"r1": f"{mod}:R1", "r2": f"{mod}:R2", "r3": R3()})
    assert resources.r1.foo() == "bar"
    assert resources["r3"].qux() == "baz"
