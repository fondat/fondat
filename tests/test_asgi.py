import pytest
import fondat.http
import http
import multidict

from dataclasses import dataclass
from fondat.asgi import asgi_app
from fondat.codec import get_codec
from fondat.resource import resource, operation
from fondat.http import InBody, Request, Response
from fondat.types import Stream, BytesStream  # , dataclass
from typing import Annotated, Any


pytestmark = pytest.mark.asyncio


class Receive:
    def __init__(self, body: bytes = b""):
        self.body = body

    async def __call__(self):
        body = dict(type=http.request, body=self.body, more_body=False)
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


async def test_simple():
    @resource
    class Resource:
        @operation
        async def get(self) -> str:
            return "str"

    app = fondat.http.Application(Resource())
    scope = dict(
        type="http",
        asgi=dict(version="3.0", spec_version="2.2"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/",
        query_string=b"",
        headers=(),
    )
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
    scope = dict(
        type="http",
        asgi=dict(version="3.0", spec_version="2.2"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/",
        query_string=b"foo=123",
        headers=(),
    )
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
    scope = dict(
        type="http",
        asgi=dict(version="3.0", spec_version="2.2"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/",
        query_string=b"foo=abc",
        headers=(),
    )
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
    scope = dict(
        type="http",
        asgi=dict(version="3.0", spec_version="2.2"),
        http_version="1.1",
        method="GET",
        scheme="http",
        path="/",
        query_string=b"",
        headers=(),
    )
    send = Send()
    await asgi_app(app)(scope, Receive(), send)
    assert send.response["status"] == http.HTTPStatus.BAD_REQUEST.value


# TODO: request with body
