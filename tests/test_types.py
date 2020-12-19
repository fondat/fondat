import pytest

from collections.abc import AsyncIterator
from fondat.types import BytesStream, str_enum


def test_str_enum_iterable():
    E = str_enum("E", ("a", "b", "c"))
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_spaces():
    E = str_enum("E", "a b c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_commas():
    E = str_enum("E", "a,b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_mixed():
    E = str_enum("E", "a,,  b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


async def _ajoin(stream: AsyncIterator[bytes]) -> bytes:
    bees = []
    async for b in stream:
        bees.append(b)
    return b"".join(bees)


@pytest.mark.asyncio
async def test_bytes_stream():
    value = b"hello"
    assert await _ajoin(BytesStream(value)) == value
