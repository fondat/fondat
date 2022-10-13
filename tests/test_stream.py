from fondat.stream import BytesStream, Reader, Stream
from random import randbytes


class ChunkyStream(Stream):
    def __init__(self, chunks: list[bytes]):
        super().__init__("content/type")
        self.chunks = chunks

    async def __anext__(self) -> bytes:
        if len(self.chunks) == 0:
            raise StopAsyncIteration
        return self.chunks.pop(0)

    async def close(self):
        pass


async def test_read_all():
    bytes1 = randbytes(1024)
    stream = BytesStream(bytes1)
    async with Reader(stream) as reader:
        bytes2 = await reader.read()
        assert bytes1 == bytes2
        assert await reader.read() == b""


async def test_read_partial():
    source = randbytes(1024)
    stream = BytesStream(source)
    async with Reader(stream) as reader:
        bytes1 = await reader.read(512)
        bytes2 = await reader.read(512)
        bytes3 = await reader.read(512)
        assert bytes1 + bytes2 == source
        assert bytes3 == b""


async def test_read_chunky():
    stream = ChunkyStream([b"12345", b"67890", b"ABCDE"])
    async with Reader(stream) as reader:
        bytes1 = await reader.read(10)
        bytes2 = await reader.read(2)
        bytes3 = await reader.read(10)
        bytes4 = await reader.read(10)
        assert bytes1 == b"1234567890"
        assert bytes2 == b"AB"
        assert bytes3 == b"CDE"
        assert bytes4 == b""


async def test_read_until_partial():
    async with Reader(BytesStream(b"hello\nworld")) as reader:
        assert await reader.read_until(b"\n") == b"hello\n"
        assert await reader.read_until(b"\n") == b"world"
        assert await reader.read_until(b"\n") == b""


async def test_read_until_none():
    async with Reader(BytesStream(b"hello world")) as reader:
        assert await reader.read_until(b"\n") == b"hello world"
        assert await reader.read_until(b"\n") == b""


async def test_read_until_chunky():
    stream = ChunkyStream([b"hel", b"lo\nwo", b"rld\n"])
    async with Reader(stream) as reader:
        assert await reader.read_until(b"\n") == b"hello\n"
        assert await reader.read_until(b"\n") == b"world\n"
        assert await reader.read_until(b"\n") == b""


async def test_read_until_long_separator():
    async with Reader(BytesStream(b"helloXXXworldXXXXXX")) as reader:
        assert await reader.read_until(b"XXX") == b"helloXXX"
        assert await reader.read_until(b"XXX") == b"worldXXX"
        assert await reader.read_until(b"XXX") == b"XXX"
        assert await reader.read_until(b"XXX") == b""
