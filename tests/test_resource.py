import fondat.schema as s
import pytest
import uuid

from fondat.resource import (
    ResourceError,
    resource,
    operation,
)


body_schema = s.dict({"id": s.uuid(), "foo": s.str()}, "foo")


@resource
class R2:
    def __init__(self, id):
        self.id = id

    @operation
    async def delete():
        pass


@resource
class R1:

    schema = body_schema

    @operation  # no params
    async def post(self, body: body_schema) -> s.uuid():
        return uuid.UUID("705d9048-97d6-4071-8359-3dbf0531fee9")

    @operation()  # empty params
    async def delete(self, id: s.uuid()):
        pass

    @operation(type="query")
    async def foo(self) -> s.str():
        return "foo"


@resource
class R3(R1):
    @operation(type="query")
    async def bar(self) -> s.str():
        return "bar"


@pytest.mark.asyncio
async def test_call():
    await R1().post(body={"foo": "bar"})


@pytest.mark.asyncio
async def test_invalid_schema():
    with pytest.raises(s.SchemaError):
        await R1().post(body=1)


def test_invalid_operation_type():
    with pytest.raises(ValueError):

        class MissingOperationTypeResource:
            @operation
            async def missing_operation_type(self):
                pass


def test_mismatch_operation_type():
    with pytest.raises(ValueError):

        class InvalidOperationTypeResource:
            @operation(type="postit")
            async def not_post(self):
                pass


def test_resource_error_str():
    assert str(ResourceError("foo")) == "foo"
