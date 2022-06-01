import pytest

from collections.abc import AsyncIterator
from fondat.stream import BytesStream
from fondat.types import is_optional, literal_values, strip_optional, union_type
from types import NoneType, UnionType
from typing import Annotated, Literal, Optional, Union, get_args, get_origin


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
    assert is_optional(str | None)
    assert is_optional(Annotated[Optional[str], ""])
    assert is_optional(Annotated[str | None, ""])
    assert is_optional(Annotated[Union[str, Annotated[Optional[int], ""]], ""])
    assert is_optional(Annotated[Union[str, Annotated[int | None, ""]], ""])
    assert is_optional(Annotated[str | Annotated[int | None, ""], ""])
    assert not is_optional(str)
    assert not is_optional(Annotated[str, ""])
    assert not is_optional(Annotated[Union[str, Annotated[int, ""]], ""])
    assert not is_optional(Annotated[str | Annotated[int, ""], ""])


def test_strip_optional():
    assert strip_optional(Optional[str]) is str
    assert strip_optional(str | None) is str
    assert strip_optional(Annotated[Optional[str], ""]) == Annotated[str, ""]
    assert strip_optional(Annotated[str | None, ""]) == Annotated[str, ""]
    assert strip_optional(Optional[Annotated[str, ""]]) == Annotated[str, ""]
    assert strip_optional(Annotated[str, ""] | None) == Annotated[str, ""]
    assert (
        strip_optional(Union[str, Annotated[Optional[int], ""]])
        == Union[str, Annotated[int, ""]]
    )
    assert (
        strip_optional(Union[str, Annotated[int | None, ""]]) == Union[str, Annotated[int, ""]]
    )
    assert strip_optional(str | Annotated[int | None, ""]) == str | Annotated[int, ""]


def test_literal_values():
    L = Literal["a", "b", "c", 1, 2, 3]
    assert literal_values(L) == {"a", "b", "c", 1, 2, 3}


def test_union_type():
    t = union_type([str, None])
    assert get_origin(t) is UnionType
    assert get_args(t) == (str, NoneType)
    assert union_type([]) is NoneType
    assert union_type([str]) is str
