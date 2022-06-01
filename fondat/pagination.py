"""
Module to support pagination of results.

Results of operations can be returned in cursor-based pages.

An operation returns a paginated result, if it:
  • accepts an optional "cursor" parameter,
  • returns a page dataclass that contains "items" and "cursor" attributes.

Example:

@dataclass
class Page:
    items: Iterable[ItemType]
    cursor: bytes | None = None
    remaining: int | None = None

@operation
async def get(self, ..., limit: int | None = None, cursor: bytes | None = None) -> Page:
    ...
    return Page(...)

An operation should establish an upper limit of items to return in each page. If appropriate,
the operation can also expose an optional "limit" parameter, which allows a caller to suggest
the number of items to be returned in the page. The operation is free to decide how many items
to return.

The "cursor" page attribute contains an opaque value that the caller supplies in a subsequent
operation call to get the next page of items. If the "cursor" attribute is None, then there are
no more items (or pages) to be requested.

The optional "remaining" page attribute contains an estimated number of items remaining after
the current page. As this value is optional, it may not be returned by the operation.
"""

from collections.abc import Callable, Coroutine, Iterable
from dataclasses import field, make_dataclass
from typing import Any


def make_page_dataclass(class_name: str, item_type: type, remaining: bool = True):
    """
    Return a page dataclass for the specified item type.

    Parameters:
    • class_name: the name to assign the dataclass
    • item_type: the type of each item in the page
    • remaining: include remaining field
    """

    fields = [
        ("items", Iterable[item_type]),
        ("cursor", bytes | None, field(default=None)),
    ]

    if remaining:
        fields.append(("remaining", int | None, field(default=None))),

    return make_dataclass(class_name, tuple(fields))


async def paginate(operation: Callable[..., Coroutine[Any, Any, Any]], /, **kwargs):
    """
    Wraps a paginated resource operation with an asynchronous generator that iterates through
    all values.

    The wrapped resource operation must return a page dataclass and accept a "cursor"
    parameter that takes an optional bytes value.

    Parameters:
    • operation: resource operation to wrap with generator
    • kwargs: keyword arguments to pass to resource operation
    """

    cursor = {}
    while cursor is not None:
        page = await operation(**kwargs, **cursor)
        cursor = {"cursor": page.cursor} if page.cursor is not None else None
        for item in page.items:
            yield item
