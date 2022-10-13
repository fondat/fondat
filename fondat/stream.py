"""Module for binary content streaming."""

from asyncio import LimitOverrunError
from collections.abc import AsyncIterator
from fondat.validation import MinLen, MinValue, validate_arguments
from typing import Annotated


class Stream(AsyncIterator[bytes | bytearray]):
    """
    Base class to provide binary content through an asynchronous stream. The stream provides
    binary data through asynchronously iterable chunks of bytes or bytearray.

    During iteration, the stream determines the size of each chunk.

    Attributes:
    • content_type: the media (MIME) type of the stream
    • content_length: the length of the content, or None if unknown

    Much like a file, a stream is returned in an "open" state. The consumer must explicitly
    close it, either via by calling its `close` method, or using `async with`.
    """

    def __init__(self, content_type: str, content_length: int | None = None):
        self.content_type = content_type
        self.content_length = content_length

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes | bytearray:
        raise NotImplementedError

    async def close(self) -> None:
        """
        Close the stream. Further attempts to iterate the stream will raise StopAsyncIteration.
        This method is idempotent; it is not an error to close a stream more than once.
        """
        raise NotImplementedError


class BytesStream(Stream):
    """
    Represents a bytes or bytearray object as an asynchronous byte stream. All content is
    returned in a single iteration.

    Parameters:
    • content: the data to be streamed
    • content_type: the MIME type of the data to be streamed
    """

    def __init__(
        self,
        content: bytes | bytearray,
        content_type: str = "application/octet-stream",
    ):
        super().__init__(content_type=content_type, content_length=len(content))
        self.content = content

    async def __anext__(self) -> bytes:
        if self.content is None:
            raise StopAsyncIteration
        result = self.content
        self.content = None
        return result

    async def close(self):
        self.content = None


class Reader:
    """
    Buffered stream reader.

    If the reader is used as an asynchronous context manager (`async with`), then upon
    exiting the context the stream will be closed.

    Parameters:
    • stream: stream to be read
    • limit: buffer size limit

    If buffer size limit is exceeded during read operations, LimitOverrunError is raised.
    """

    def __init__(self, stream: Stream, limit: int | None = None):
        self.stream = stream
        self.limit = limit
        self._buffer = bytearray()
        self._eof = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _read(self):
        try:
            self._buffer += await self.stream.__anext__()
            if self.limit and len(self._buffer) > self.limit:
                raise LimitOverrunError
        except StopAsyncIteration:
            self._eof = True

    @validate_arguments
    async def read(self, size: Annotated[int, MinValue(1)] | None = None) -> bytes:
        """
        Read bytes from the stream.

        Parameter:
        • size: number of bytes to read  [to end of stream]

        This method blocks until all requested bytes are read or the end of the stream is
        encountered.

        The end of stream is signified by zero bytes returned.
        """
        while not self._eof and (size is None or len(self._buffer) < size):
            await self._read()
        result = bytes(self._buffer[:size] if size else self._buffer)
        self._buffer = self._buffer[size:] if size else bytearray(b"")
        return result

    @validate_arguments
    async def read_until(self, separator: Annotated[bytes, MinLen(1)]) -> bytes:
        """
        Read bytes from the stream, up to and including the specified separator.

        Parameter:
        • separator: byte sequence to read up to

        This method blocks until all requested bytes are read or the end of the stream is
        encountered. If it reaches the end of the stream, all bytes read are returned.

        The end of stream is signified by zero bytes returned.
        """
        while not self._eof and separator not in self._buffer:
            await self._read()
        index = self._buffer.find(separator)
        size = index + len(separator) if index != -1 else None
        result = bytes(self._buffer[:size] if size else self._buffer)
        self._buffer = self._buffer[size:] if size else bytearray(b"")
        return result

    async def close(self):
        """Close the stream."""
        await self.stream.close()


async def read_stream(stream: Stream, limit: int | None = None) -> bytes:
    """Deprecated. Use Reader class."""
    return await Reader(stream, limit).read()
