"""Module for content streaming."""

import mimetypes

from collections.abc import AsyncIterator
from pathlib import Path


class Stream(AsyncIterator[bytes | bytearray]):
    """
    Base class to represent content as an asynchronous stream of bytes.

    Parameters and attributes:
    • content_type: the media type of the stream
    • content_length: the length of the content, if known
    """

    def __init__(
        self,
        content_type: str = "application/octet-stream",
        content_length: int | None = None,
    ):
        self.content_type = content_type
        self.content_length = content_length

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes | bytearray:
        raise NotImplementedError


class BytesStream(Stream):
    """
    Represents a bytes or bytearray object as an asynchronous byte stream. All bytes are
    returned in a single iteration.

    Parameters:
    • content: ...
    • content_type: ...
    """

    def __init__(
        self,
        content: bytes | bytearray,
        content_type: str = "application/octet-stream",
    ):
        super().__init__(content_type, len(content))
        self._content = content

    async def __anext__(self) -> bytes:
        if self._content is None:
            raise StopAsyncIteration
        result = self._content
        self._content = None
        return result


async def stream_bytes(stream: Stream) -> bytearray:
    """Read a stream and return the contents in a byte array."""

    if stream is None:
        return None
    result = bytearray()
    async for b in stream:
        result.extend(b)
    return result
