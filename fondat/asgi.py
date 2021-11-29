"""Module to expose resources through ASGI."""

import fondat.http
import logging
import urllib.parse

from collections.abc import Awaitable, Callable, Mapping
from fondat.error import InternalServerError
from fondat.stream import Stream


_logger = logging.getLogger(__name__)


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
            raise StopAsyncIteration
        if event_type != "http.request":
            raise InternalServerError(
                f"expecting http.request event type; received {event_type}"
            )
        self._more = event.get("more_body", False)
        return event.get("body", b"")


def asgi_app(
    handler: Callable, startup: Callable = None, shutdown: Callable = None
) -> Callable:
    """
    Expose a Fondat HTTP request handler as an ASGI application.

    Parameters:
    • handler: HTTP handler coroutine function
    • startup: lifespan startup coroutine function
    • shutdown: lifespan shutdown coroutine function

    The HTTP handler coroutine function is called in response to ASGI HTTP protocol events.

    The startup and shutdown coroutine functions are called in response to ASGI lifespan
    protocol events. This allows the application to initialize and shutdown in the context of
    a running event loop.
    """

    async def lifespan(scope, receive, send):
        message = await receive()
        lifespan_type = message["type"]
        try:
            if lifespan_type == "lifespan.startup":
                if startup is not None:
                    await startup()
            elif lifespan_type == "lifespan.shutdown":
                if shutdown is not None:
                    await shutdown()
            else:
                raise InternalServerError(f"unknown ASGI lifespan type: {lifespan_type}")
        except Exception as e:
            _logger.exception(f"ASGI {lifespan_type} failed")
            await send({"type": f"{lifespan_type}.failed", "message": str(e)})
            raise
        await send({"type": f"{lifespan_type}.complete"})

    async def http(scope, receive, send):
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
        headers = ((k.lower().encode(), v.encode()) for k, v in response.headers.items())
        headers = [*headers]
        cookies = (
            (
                (k.lower().encode(), v.encode())
                for k, v in (
                    header.split(": ")
                    for header in response.cookies.output(sep="\n").split("\n")
                )
            )
            if response.cookies
            else ()
        )
        cookies = [*cookies]
        await send(
            {
                "type": "http.response.start",
                "status": response.status,
                "headers": [*headers, *cookies],
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

    async def app(scope, receive, send):
        """Coroutine that implements ASGI interface."""
        scope_type = scope["type"]
        if scope_type == "http":
            return await http(scope, receive, send)
        elif scope_type == "lifespan":
            return await lifespan(scope, receive, send)
        else:
            raise InternalServerError(f"unknown ASGI scope type: {scope_type}")

    return app
