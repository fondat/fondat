import pytest
import fondat.http
import http
import multidict

from dataclasses import dataclass
from fondat.asgi import asgi_app
from fondat.codec import get_codec
from fondat.resource import resource, operation
from fondat.http import AsBody, Request, Response
from fondat.types import Stream, BytesStream  # , dataclass
from typing import Annotated, Any, Optional


pytestmark = pytest.mark.asyncio


class Receive:
    def __init__(self, body: bytes = b""):
        self.body = body

    async def __call__(self):
        body = dict(type="http.request", body=self.body, more_body=False)
        self.body = b""
        return body


class Send:
    def __init__(self):
        self.response = {}
        self.body = bytearray()

    async def __call__(self, msg: dict[str, Any]):
        if msg["type"] == "http.response.start":
            self.response = msg
        elif msg["type"] == "http.response.body":
            self.body.extend(msg.get("body", b""))


def _scope(*, method: str, path: str = "/"):
    return dict(
        type="http",
        asgi=dict(version="3.0", spec_version="2.2"),
        http_version="1.1",
        method=method,
        scheme="http",
        path=path,
        headers=(),
    )


async def test_simple():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    app = fondat.http.Application(Resource())
    scope = {
        **_scope(method="GET", path="/"),
        "headers": (
            (b"host", b"localhost:1234"),
            (b"user-agent", b"Fonzilla/1.0"),
        ),
    }
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    response = send.response
    assert response["status"] == http.HTTPStatus.OK.value
    headers = dict(response["headers"])
    assert headers[b"content-type"] == b"text/plain; charset=UTF-8"
    assert headers[b"content-length"] == b"3"
    assert send.body == b"str"


async def test_valid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    app = fondat.http.Application(Resource())
    scope = {**_scope(method="GET", path="/"), "query_string": b"foo=123"}
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.OK.value
    assert send.body == b"123"


async def test_invalid_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    app = fondat.http.Application(Resource())
    scope = {**_scope(method="GET", path="/"), "query_string": b"foo=abc"}
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


async def test_missing_param():
    @resource
    class Resource:
        @operation
        async def get(self, foo: int) -> str:
            return str(foo)

    app = fondat.http.Application(Resource())
    scope = _scope(method="GET", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


async def test_valid_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[str, AsBody]) -> str:
            return foo

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    body = b"I am the body of this message."
    await asgi_app(app)(scope, Receive(body), send)
    assert send.response["status"] == http.HTTPStatus.OK.value
    assert send.body == body


async def test_invalid_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[int, AsBody]) -> str:
            return "str"

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    body = b"This is not an int."
    await asgi_app(app)(scope, Receive(body), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


async def test_empty_required_str_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[str, AsBody]) -> str:
            return foo

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


async def test_empty_required_int_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[int, AsBody]) -> str:
            return "str"

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


async def test_empty_default_optional_str_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[str, AsBody] = None) -> str:
            return str(foo)

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.OK.value
    assert send.body == b"None"


async def test_empty_optional_int_body_param():
    @resource
    class Resource:
        @operation
        async def post(self, foo: Annotated[Optional[int], AsBody]) -> str:
            return str(foo)

    app = fondat.http.Application(Resource())
    scope = _scope(method="POST", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.OK.value
    assert send.body == b"None"


async def test_cookie():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "foo"

    async def filter(request):
        response = yield
        response.cookies["x"] = "y"
        yield response

    app = fondat.http.Application(root=Resource(), filters=[filter])
    scope = _scope(method="GET", path="/")
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    headers = dict(send.response["headers"])
    assert headers[b"set-cookie"] == b"x=y"
