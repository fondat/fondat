"""Module to store resource items in files."""

from __future__ import annotations

import aiofiles
import logging
import os
import os.path

from collections.abc import Iterable
from fondat.codec import Binary, String, get_codec
from fondat.error import InternalServerError, NotFoundError
from fondat.http import InBody
from fondat.paging import make_page_dataclass
from fondat.resource import resource, operation
from fondat.types import affix_type_hints
from fondat.security import SecurityRequirement
from typing import Annotated, Any
from urllib.parse import quote, unquote


_logger = logging.getLogger(__name__)


def _file_resource_class(
    value_type: type,
    compress: Any = None,
    read_only: bool = False,
    security: Iterable[SecurityRequirement] = None,
):

    codec = get_codec(Binary, value_type)

    @resource
    class FileResource:
        def __init__(self, path):
            self.path = path

        @operation(security=security)
        async def get(self) -> value_type:
            """Read file."""
            try:
                async with aiofiles.open(self.path, "rb") as file:
                    content = await file.read()
                if compress:
                    content = compress.decompress(content)
                return codec.decode(content)
            except FileNotFoundError:
                raise NotFoundError
            except (TypeError, ValueError) as e:
                raise InternalServerError from e

        if not read_only:

            @operation(security=security)
            async def put(self, value: Annotated[value_type, InBody]):
                """Write file."""
                tmp = self.path + ".__tmp"
                content = codec.encode(value)
                if compress:
                    content = compress.compress(content)
                try:
                    async with aiofiles.open(tmp, "xb") as file:
                        await file.write(content)
                    os.replace(tmp, self.path)
                except Exception as e:
                    raise InternalServerError(f"cannot write file: {self.path}") from e

            @operation(security=security)
            async def delete(self):
                """Delete file."""
                try:
                    os.remove(self.path)
                except FileNotFoundError:
                    raise NotFoundError

    affix_type_hints(FileResource, localns=locals())
    FileResource.__qualname__ = "FileResource"
    return FileResource


def file_resource(
    path: str,
    value_type: type,
    compress: Any = None,
    read_only: bool = False,
    security: Iterable[SecurityRequirement] = None,
) -> Any:
    """
    Return a new resource that manages a file.

    Parameters:
    • path: path in filesystem to file
    • value_type: type of value stored in file
    • compress: algorithm to compress and decompress file content
    • read_only: can file only be read
    • security: security requirements to apply to file operations

    Compression algorithm is any object or module that exposes callable
    "compress" and "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """
    return _file_resource_class(
        value_type=value_type, compress=compress, read_only=read_only, security=security
    )(path)


def _limit(requested):
    upper = 1000
    if not requested or requested < 0:
        return upper
    return min(requested, upper)


def directory_resource(
    path: str,
    key_type: type,
    value_type: type,
    extension: str = None,
    compress: Any = None,
    security: Iterable[SecurityRequirement] = None,
) -> Any:
    """
    Return a new resource that manages files in a directory.

    Parameters:
    • path: path to directory where files are stored
    • key_type: type of key to identify file
    • value_type: type of value stored in each file
    • extenson: filename extension to use for each file (including dot)
    • compress: algorithm to compress and decompress file content
    • security: Security requirements to apply to all operations

    Compression algorithm is any object or module that exposes callable
    "compress" and "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """

    codec = get_codec(String, key_type)
    _path = os.path.expanduser((path).rstrip("/"))
    os.makedirs(_path, exist_ok=True)

    Page = make_page_dataclass("Page", value_type)

    FileResource = _file_resource_class(
        value_type=value_type, compress=compress, security=security
    )

    @resource
    class DirectoryResource:
        @operation(security=security)
        async def get(self, limit: int = None, cursor: bytes = None) -> Page:
            """Return paginated list of file keys."""
            limit = _limit(limit)
            if cursor is not None:
                cursor = cursor.decode()
            try:
                with os.scandir(_path) as entries:
                    names = sorted(
                        entry.name[: -len(extension)] if extension else entry.name
                        for entry in entries
                        if entry.is_file()
                        and (not extension or entry.name.endswith(extension))
                    )
            except FileNotFoundError:
                raise InternalServerError(f"directory not found: {_path}")
            page = Page(items=[], cursor=None, remaining=0)
            for (counter, name) in enumerate(names, 1):
                if cursor is not None:
                    if name <= cursor:
                        continue
                    cursor = None
                try:
                    page.items.append(codec.decode(unquote(name)))
                except ValueError:
                    continue  # ignore name that cannot be decoded
                if len(page.items) == limit and counter < len(names):
                    page.cursor = name.encode()
                    page.remaining = len(names) - counter
                    break
            return page

        def __getitem__(self, key: key_type) -> FileResource:
            return FileResource(
                f"{_path}/{quote(codec.encode(key), safe='')}{extension if extension else ''}"
            )

    affix_type_hints(DirectoryResource, localns=locals())
    DirectoryResource.__qualname__ = "DirectoryResource"

    return DirectoryResource()
