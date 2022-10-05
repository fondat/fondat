"""Module for binary content streaming."""

from collections.abc import AsyncIterator


class Stream(AsyncIterator[bytes | bytearray]):
    """
    Base class to provide binary content through an asynchronous stream. The stream provides
    binary data in asynchronously iterable chunks of bytes or bytearray.

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


async def read_stream(stream: Stream, limit: int | None = None) -> bytearray:
    """
    Read the entire content from a stream and return it in a byte array. This function does
    not close the stream after all bytes are read.

    Parameters:
    • stream: stream to read binary data from
    • limit: byte array length limit  [no limit]

    If the size byte array would exceed the specified limit, `ValueError` is raised.
    """

    result = bytearray()
    async for chunk in stream:
        result += chunk
        if limit and len(result) > limit:
            raise ValueError("byte array length limit exceeded")
    return result
