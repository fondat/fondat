import pytest

from dataclasses import field
from collections.abc import AsyncIterator
from fondat.types import BytesStream, dataclass
from typing import Optional


async def _ajoin(stream: AsyncIterator[bytes]) -> bytes:
    bees = []
    async for b in stream:
        bees.append(b)
    return b"".join(bees)


@pytest.mark.asyncio
async def test_bytes_stream():
    value = b"hello"
    assert await _ajoin(BytesStream(value)) == value


def test_dataclass_optional():
    @dataclass
    class Foo:
        x: Optional[int]
    foo = Foo()
    assert foo.x == None


def test_dataclass_default():
    @dataclass
    class Foo:
        x: int = 1
    foo = Foo()
    assert foo.x == 1


def test_dataclass_field_default():
    @dataclass
    class Foo:
        x: int = field(default=1)
    foo = Foo()
    assert foo.x == 1   


def test_dataclass_field_default_factory():
    @dataclass
    class Foo:
        x: dict = field(default_factory=dict)
    foo = Foo()
    assert foo.x == {}
