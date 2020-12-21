"""Fondat ASGI module."""

import fondat.http
import urllib.parse

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
        headers = scope.get("headers", {})
        self.content_type = headers.get("content-type")
        self.content_length = _int(headers.get("content-length"))
        self._receive = receive
        self._more = True

    async def __anext__(self) -> bytes:
        if not self.more:
            raise StopAsyncIteration
        event = await self._receive()
        event_type = event["type"]
        if event_type == "http.disconnect":
            raise RuntimeError  # TODO: better error type? CancelException?
        if event_type != "http.request.body":
            raise TypeError("expecting HTTP request body")  # TODO: better error type?
        self._more = event.get("more_body", False)
        return event.get("body", b"")


class Application(fondat.http.Application):

    async def app(self, scope, receive, send):
        """Coroutine that implements ASGI interface."""
        if scope["type"] != "http":
            raise ValueError("expecting http scope")  # TODO: better error type?
        request = fondat.http.Request()
        request.method = scope["method"]
        request.path = scope["path"]
        headers = fondat.http.Headers(
            ((k.decode(), v.decode()) for k, v in scope.get("headers") or ())
        )
        for header in headers.popall("cookie") or ():
            request.cookies.load(header)
        request.headers = headers
        request.query = fondat.http.Query(
            urllib.parse.parse_qsl((scope.get("query_string") or b"").decode()), True
        )
        request.body = ReceiveStream(scope, receive)
        response = await self.handle(request)
        await send(
            {
                "type": "http.response.start",
                "status": response.status,
                # FIXME: add cookies
                "headers": (
                    (k.encode().lower(), v.encode()) for k, v in headers.items()
                ),
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
