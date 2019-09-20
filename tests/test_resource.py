import pytest
import roax.schema as s
import uuid

from roax.resource import Resource, Resources, operation


body_schema = s.dict({"id": s.uuid(), "foo": s.str()}, "foo")


class R1(Resource):

    schema = body_schema

    @operation  # no params
    def create(self, body: body_schema) -> s.uuid():
        return uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9")

    @operation()  # empty params
    def delete(self, id: s.uuid()):
        pass

    @operation(type="query")
    def foo(self) -> s.str():
        return "bar"


class R3(Resource):
    @operation(type="query")
    def qux(self) -> s.str():
        return "baz"


class InvalidOperationTypeResource(Resource):
    def __init__(self):
        super().__init__()
        self.not_a_valid_operation_type = operation()(self.not_a_valid_operation_type)

    def not_a_valid_operation_type(self):
        pass


def test_r1_operations():
    r1 = R1()
    assert r1.operations["create"].name == "create"
    assert r1.operations["foo"].type == "query"


def test_call():
    result = R1().operations["create"].call(body={"foo": "bar"})


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
    resources = Resources({"r1": f"{mod}:R1", "r3": R3()})
    assert resources.r1.foo() == "bar"
    assert resources["r3"].qux() == "baz"
