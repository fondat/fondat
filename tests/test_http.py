import pytest
import http

from fondat.resource import resource, operation
from fondat.http import Application, Request, Response


pytestmark = pytest.mark.asyncio


async def body(message):
    """Extract body from message."""
    return b"".join([b async for b in message.body])


async def test_simple():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    application = Application(Resource(), "", "", "", "")

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain"
    assert response.headers["Content-Length"] == "3"
    assert await body(response) == b"str"


async def test_nested():
    @resource
    class Nested:
        @operation
        async def get(self) -> str:
            return "nested"

    @resource
    class Root:
        nested = Nested()

    application = Application(Root(), "", "", "", "")

    request = Request()
    request.method = "GET"
    request.path = "/nested"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain"
    assert response.headers["Content-Length"] == "6"
    assert await body(response) == b"nested"


async def test_valid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource(), "", "", "", "")

    request = Request()
    request.method = "GET"
    request.path = "/"
    request.query["foo"] = "123"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert await body(response) == b"123"


async def test_invalid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource(), "", "", "", "")

    request = Request()
    request.method = "GET"
    request.path = "/"
    request.query["foo"] = "abc"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value


async def test_missing_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource(), "", "", "", "")

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value
