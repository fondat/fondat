"""
Module to support pagination of items.

For an operation that returns a large set of items, it can be expensive to return all items in
a single response. In this case, the operation should return items in pages, requiring multiple
calls to the operation to retrieve all items.

Paginated items are provided through a page dataclass, which minimally contains:

  • an iterable object of `items` in the page
  • an opaque `cursor` value to retrieve the next page

The caller will initially pass no cursor value to an operation, resulting in the first page
of items. When generating the page, if there are additional items to be returned, the page
cursor should contain an opaque value that can be used to request the subsequent page. The
last page should contain no cursor value, indicating there are no further items to request.

An operation should establish a reasonable limit of items it would return in each page. If
appropriate, the operation can also expose a `limit` parameter, which allows a caller to
request a number of items to be returned in the page. Regardless of how many items are
requested, the operation should be free to decide how many items to actually return.
"""

from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar


class PaginationError(Exception):
    """
    Error raised if pagination could not be performed.

    An example of a pagination error is a loss of indexing: pages have drifted due to changes
    to the underlying data between page requests, and the continuation point of the next page
    cannot be reconstructed.
    """


# type aliases
Item = TypeVar("Item")
Cursor = bytes | None


@dataclass
class Page(Generic[Item]):
    """A paginated result."""

    items: Iterable[Item]
    cursor: Cursor = None


async def paginate(operation: Callable[..., Coroutine[Any, Any, Any]], /, **kwargs):
    """
    Wraps a paginated resource operation with an asynchronous generator that iterates through
    all items. The wrapped resource operation must return a page dataclass and accept a
    `cursor` parameter.

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
