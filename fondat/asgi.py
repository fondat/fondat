"""Module to expose resources through ASGI."""

import fondat.http
import urllib.parse

from collections.abc import Awaitable, Mapping
from fondat.types import Stream


def _int(s: str):
    result = None
    if s is not None:
        try:
            return int(s)
        except:
            pass
    return result


class ReceiveStream(Stream):
    """Stream that encapsulates the ASGI receive interface."""

    def __init__(self, scope: Mapping, receive: Awaitable):
        for key, value in scope.get("headers"):
            if key == "content-type":
                self.content_type = value
            elif key == "content-length":
                self.content_length = _int(value)
        self._receive = receive
        self._more = True

    async def __anext__(self) -> bytes:
        if not self._more:
            raise StopAsyncIteration
        event = await self._receive()
        event_type = event["type"]
        if event_type == "http.disconnect":
            raise RuntimeError  # TODO: better error type? CancelException?
        if event_type != "http.request.body":
            raise InternalSererError("expecting HTTP request body")
        self._more = event.get("more_body", False)
        return event.get("body", b"")


def asgi_app(handler: Awaitable):
    """Expose a Fondat HTTP request handler as an ASGI application."""

    async def app(scope, receive, send):
        """Coroutine that implements ASGI interface."""
        if scope["type"] != "http":
            raise InternalServerError("expecting http scope")
        request = fondat.http.Request()
        request.method = scope["method"]
        request.path = scope["path"]
        request.version = scope["http_version"]
        for key, value in scope["headers"]:
            request.headers.add(key.decode(), value.decode())
        for header in request.headers.popall("cookie", ()):
            request.cookies.load(header)
        request.query = fondat.http.Query(
            urllib.parse.parse_qsl((scope.get("query_string") or b"").decode())
        )
        request.body = ReceiveStream(scope, receive)
        response = await handler(request)
        await send(
            {
                "type": "http.response.start",
                "status": response.status,
                # FIXME: add cookies
                "headers": [
                    (k.lower().encode(), v.encode()) for k, v in response.headers.items()
                ],
            }
        )
        if response.body is not None:
            async for chunk in response.body:
                await send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
        await send(
            {
                "type": "http.response.body",
                "more_body": False,
            }
        )

    return app
