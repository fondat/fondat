"""
Fondat paging module.

This module provides helpers to implement cursor-based pagination.

An operation returns a paginated result, if it:
  • accepts an optional "cursor" parameter,
  • returns a page dataclass that contains "items" and "cursor" attributes.

Example:

@dataclass
class Page:
    items: Iterable[ItemType]
    cursor: Optional[bytes] = None
    remaining: Optional[int] = None

@operation
async def get(self, ..., limit: int = None, cursor: bytes = None) -> Page:
    ...

An operation should establish an upper limit of items to return in each page.
If appropriate, the operation can also expose an optional "limit" parameter,
which allows a caller to request a maximum number of items to be returned in
the page. The operation should return no more items than requested, but can
always elect to return less. If not specified, the operation should return an
optimal number of items in a page.

The "cursor" page dataclass attribute contains an opaque value that the caller
supplies in a subsequent operation call to get the next page of items. If the
"cursor" page attribute is None, then there are no more items (or pages) to be
requested.

The optional "remaining" page dataclass attribute contains an estimated number
of items that are remaining after the current page. As this value is optional,
it may not be returned by the operation.
"""

from collections.abc import Callable, Iterable
from dataclasses import make_dataclass, field
from typing import Optional


def make_page_dataclass(class_name: str, item_type: type):
    """
    Return a page dataclass for the specified item type.

    Parameters:
    • class_name: the name to assign the dataclass
    • item_type: the type of each item in the page
    """

    return make_dataclass(
        class_name,
        (
            ("items", Iterable[item_type]),
            ("cursor", Optional[bytes], field(default=None)),
            ("remaining", Optional[int], field(default=None)),
        ),
    )


async def paginate(operation: Callable, cursor: bytes = None, /, *args, **kwargs):
    """
    Wraps a paginated resource operation with an asynchronous generator that
    iterates through all values.

    The wrapped resource operation must return a page dataclass and accept a
    "cursor" parameter that takes an optional bytes value.

    Parameters:
    • operation: resource operation to wrap with generator
    • cursor: initial cursor position to begin pagination
    • args: positional arguments to pass to resource operation
    • kwargs: keyword arguments to pass to resource operation
    """

    cursor = {"cursor": cursor} if cursor is not None else {}

    while cursor is not None:
        page = await operation(**kwargs, **cursor)
        cursor = {"cursor": page.cursor} if page.cursor is not None else None
        for item in page.items:
            yield item
