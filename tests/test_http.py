import pytest
import http

from typing import Annotated
from fondat.codec import Binary, get_codec
from fondat.resource import resource, operation
from fondat.http import Application, InBody, Request, Response
from fondat.types import Stream, BytesStream  # , dataclass
from dataclasses import dataclass

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

    application = Application(Resource())

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain; charset=UTF-8"
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

    application = Application(Root())

    request = Request()
    request.method = "GET"
    request.path = "/nested"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain; charset=UTF-8"
    assert response.headers["Content-Length"] == "6"
    assert await body(response) == b"nested"


async def test_valid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource())

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

    application = Application(Resource())

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

    application = Application(Resource())

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value


async def test_stream_response_body():
    @resource
    class Resource:
        @operation
        async def get(self) -> Stream:
            return BytesStream(b"12345")

    application = Application(Resource())

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert await body(response) == b"12345"


async def test_stream_request_body():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[Stream, InBody]) -> BytesStream:
            content = b"".join([b async for b in foo])
            return BytesStream(content)

    application = Application(Resource())

    content = b"abcdefg"
    request = Request()
    request.method = "POST"
    request.path = "/"
    request.body = BytesStream(content)
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Length"] == str(len(content))
    assert await body(response) == content


async def test_request_body_dataclass():
    @dataclass
    class Model:
        a: int
        b: str

    @resource
    class Resource:
        @operation
        async def post(self, val: Annotated[Model, InBody]) -> Model:
            return val

    application = Application(Resource())

    m = Model(a=1, b="s")
    codec = get_codec(Binary, Model)

    request = Request()
    request.method = "POST"
    request.path = "/"
    request.body = BytesStream(codec.encode(m))
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert codec.decode(await body(response)) == m


async def test_invalid_return():
    @resource
    class Resource:
        @operation
        async def get(self) -> int:
            return "str"

    application = Application(Resource())

    request = Request()
    request.method = "GET"
    request.path = "/"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.INTERNAL_SERVER_ERROR.value
