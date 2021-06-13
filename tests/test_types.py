import pytest

from collections.abc import AsyncIterator
from fondat.stream import BytesStream
from fondat.types import is_optional, strip_optional
from typing import Annotated, Optional, Union


async def _ajoin(stream: AsyncIterator[bytes]) -> bytes:
    bees = []
    async for b in stream:
        bees.append(b)
    return b"".join(bees)


@pytest.mark.asyncio
async def test_bytes_stream():
    value = b"hello"
    assert await _ajoin(BytesStream(value)) == value


def test_is_optional():
    assert is_optional(Optional[str])
    assert is_optional(Annotated[Optional[str], ""])
    assert is_optional(Annotated[Union[str, Annotated[Optional[int], ""]], ""])
    assert not is_optional(str)
    assert not is_optional(Annotated[str, ""])
    assert not is_optional(Annotated[Union[str, Annotated[int, ""]], ""])


def test_strip_optional():
    assert strip_optional(Optional[str]) is str
    assert (
        strip_optional(Union[str, Annotated[Optional[int], ""]])
        == Union[str, Annotated[int, ""]]
    )
