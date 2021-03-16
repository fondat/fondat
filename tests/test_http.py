import pytest
import fondat.error
import http

from typing import Annotated
from fondat.codec import Binary, get_codec
from fondat.resource import resource, operation
from fondat.http import Application, AsBody, InBody, Request, Response
from fondat.types import Stream, BytesStream
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
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain; charset=UTF-8"
    assert response.headers["Content-Length"] == "3"
    assert await body(response) == b"str"


async def test_nested_attr():
    @resource
    class Nested:
        @operation
        async def get(self) -> str:
            return "nested"

    @resource
    class Root:
        nested = Nested()

    application = Application(Root())
    request = Request(method="GET", path="/nested")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain; charset=UTF-8"
    assert response.headers["Content-Length"] == "6"
    assert await body(response) == b"nested"


async def test_nested_item():
    @resource
    class Inner:
        def __init__(self, key: str):
            self.key = key

        @operation
        async def get(self) -> str:
            return self.key

    @resource
    class Outer:
        def __getitem__(self, key: str) -> Inner:
            return Inner(key)

    app = Application(Outer())
    request = Request(method="GET", path="/abc")
    response = await app.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert response.headers["Content-Type"] == "text/plain; charset=UTF-8"
    assert await body(response) == b"abc"


async def test_valid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource())
    request = Request(method="GET", path="/")
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
    request = Request(method="GET", path="/")
    request.query["foo"] = "abc"
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value


async def test_missing_required_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    application = Application(Resource())
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value


async def test_missing_optional_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int = None) -> str:
            return str(foo)

    application = Application(Resource())
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert await body(response) == b"None"


async def test_stream_response_body():
    @resource
    class Resource:
        @operation
        async def get(self) -> Stream:
            return BytesStream(b"12345")

    application = Application(Resource())
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert await body(response) == b"12345"


async def test_stream_request_body():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[Stream, AsBody]) -> BytesStream:
            content = b"".join([b async for b in foo])
            return BytesStream(content)

    application = Application(Resource())
    content = b"abcdefg"
    request = Request(method="POST", path="/", body=BytesStream(content))
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
        async def post(self, val: Annotated[Model, AsBody]) -> Model:
            return val

    application = Application(Resource())
    m = Model(a=1, b="s")
    codec = get_codec(Binary, Model)
    request = Request(method="POST", path="/", body=BytesStream(codec.encode(m)))
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
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.INTERNAL_SERVER_ERROR.value


async def test_filter_return():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    async def filter(request):
        return Response(status=http.HTTPStatus.FORBIDDEN.value)

    application = Application(root=Resource(), filters=[filter])
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.FORBIDDEN.value


async def test_filter_yield():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    async def filter(request):
        assert request.method == "GET"
        response = yield
        assert response.status == http.HTTPStatus.OK.value

    application = Application(root=Resource(), filters=[filter])
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value


async def test_filter_yield_raises_exception():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            raise fondat.error.NotFoundError

    async def filter(request):
        try:
            yield
        except fondat.error.NotFoundError as e:
            raise fondat.error.InternalServerError from e

    application = Application(root=Resource(), filters=[filter])
    request = Request(method="GET", path="/")
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.INTERNAL_SERVER_ERROR.value


async def test_request_in_body_parameters():
    @resource
    class Resource:
        @operation
        async def post(self, a: Annotated[str, InBody], b: Annotated[str, InBody]) -> str:
            return f"{a}{b}"

    application = Application(Resource())
    request = Request(method="POST", path="/", body=BytesStream(b'{"a": "foo", "b": "bar"}'))
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.OK.value
    assert await body(response) == b"foobar"


async def test_body_validation():
    @resource
    class Resource:
        @operation
        async def post(self, a: Annotated[int, InBody]) -> str:
            return f"{a}"

    application = Application(Resource())
    request = Request(method="POST", path="/", body=BytesStream(b'{"a": "not_int"}'))
    response = await application.handle(request)
    assert response.status == http.HTTPStatus.BAD_REQUEST.value
