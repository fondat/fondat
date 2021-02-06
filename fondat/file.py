"""Module to store resource items in files."""

from __future__ import annotations

import aiofiles
import logging
import mimetypes
import os
import os.path

from collections.abc import Iterable
from fondat.codec import Binary, String, get_codec
from fondat.error import InternalServerError, NotFoundError
from fondat.http import InBody
from fondat.pagination import make_page_dataclass
from fondat.resource import resource, operation
from fondat.types import Stream, affix_type_hints
from fondat.security import SecurityRequirement
from typing import Annotated, Any
from urllib.parse import quote, unquote


_logger = logging.getLogger(__name__)


_content_encoding_map = {
    "br": "application/x-br",
    "bzip2": "application/x-bzip2",
    "compress": "application/x-compress",
    "gzip": "application/gzip",
    ".xz": "application/x-xz",
}


def _content_type(path: str) -> str:
    content_type, content_encoding = mimetypes.guess_type(path)
    if content_encoding:
        return _content_encoding_map.get(content_encoding, content_encoding)
    if content_type:
        return content_type
    return "application/octet-stream"


class _ReadFileStream(Stream):
    def __init__(self, path: str, block_size: int = 131072):
        super().__init__(_content_type(path))
        self.path = path
        self.block_size = block_size
        self.position = 0

    async def __anext__(self) -> bytes:
        if not self.path:
            raise StopAsyncIteration
        async with aiofiles.open(self.path, "rb") as file:
            await file.seek(self.position)
            block = await file.read1(self.block_size)
        if len(block) == 0:
            raise StopAsyncIteration
        self.position += len(block)
        return block


def _stream_resource_class(
    writeable: bool,
    publish: bool,
    security: Iterable[SecurityRequirement],
):
    @resource
    class StreamResource:
        def __init__(self, path):
            self.path = path

        @operation(publish=publish, security=security)
        async def get(self) -> Stream:
            """Read resource."""
            if not os.path.isfile(self.path):
                raise NotFoundError
            return _ReadFileStream(self.path)

        if writeable:

            @operation(publish=publish, security=security)
            async def put(self, value: Annotated[Stream, InBody]):
                """Write resource."""
                tmp = self.path + ".__tmp"
                try:
                    async with aiofiles.open(tmp, "xb") as file:
                        async for block in value:
                            file.write(block)
                    os.replace(tmp, self.path)
                except Exception as e:
                    raise InternalServerError(f"cannot write file: {self.path}") from e

            @operation(publish=publish, security=security)
            async def delete(self):
                """Delete resource."""
                try:
                    os.remove(self.path)
                except FileNotFoundError:
                    raise NotFoundError

    affix_type_hints(StreamResource, localns=locals())
    StreamResource.__qualname__ = "StreamResource"
    return StreamResource


def _file_resource_class(
    value_type: type,
    compress: Any,
    writeable: bool,
    publish: bool,
    security: Iterable[SecurityRequirement],
):

    if value_type is Stream:
        if compress is not None:
            raise TypeError("file resources does not support compression of streams")
        return _stream_resource_class(writeable, publish, security)

    codec = get_codec(Binary, value_type)

    @resource
    class FileResource:
        def __init__(self, path):
            self.path = path

        @operation(publish=publish, security=security)
        async def get(self) -> value_type:
            """Read resource."""
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

        if writeable:

            @operation(publish=publish, security=security)
            async def put(self, value: Annotated[value_type, InBody]):
                """Write resource."""
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

            @operation(publish=publish, security=security)
            async def delete(self):
                """Delete resource."""
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
    writeable: bool = False,
    publish: bool = True,
    security: Iterable[SecurityRequirement] = None,
) -> type:
    """
    Return a new resource that manages a file.

    Parameters:
    • path: path in filesystem of file
    • value_type: type of value stored in file
    • compress: algorithm to compress and decompress file content
    • writeable: can file be written or deleted
    • publish: publish the operation in documentation
    • security: security requirements to apply to file operations

    Compression algorithm is any object or module that exposes callable "compress" and
    "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """
    return _file_resource_class(value_type, compress, writeable, publish, security)(path)


def _limit(requested):
    upper = 1000
    if not requested or requested < 0:
        return upper
    return min(requested, upper)


def directory_resource(
    path: str,
    key_type: type,
    value_type: type = Stream,
    extension: str = None,
    compress: Any = None,
    writeable: bool = False,
    index: Union[type, str] = None,
    publish: bool = True,
    security: Iterable[SecurityRequirement] = None,
) -> type:
    """
    Return a new resource that manages files in a directory.

    Parameters:
    • path: path to directory where files are stored
    • key_type: type of key to identify file
    • value_type: type of value stored in each file
    • extenson: filename extension to append (including dot)
    • compress: algorithm to compress and decompress file content
    • writeable: can files be written or deleted
    • index: index file key to represent directory
    • publish: publish the operation in documentation
    • security: Security requirements to apply to all operations

    Compression algorithm is any object or module that exposes callable "compress" and
    "decompress" attributes. Examples: bz2, gzip, lzma, zlib. Compression is not supported for
    value_type of Stream.

    The index parameter can be one of the following:
    • key_type: identifies a file in the directory to provide as the index
    • None: generates a paginated list of file keys as the index
    """

    codec = get_codec(String, key_type)
    _path = os.path.expanduser((path).rstrip("/"))
    os.makedirs(_path, exist_ok=True)

    Page = make_page_dataclass("Page", value_type)

    FileResource = _file_resource_class(value_type, compress, writeable, publish, security)

    @resource
    class DirectoryResource:

        if index is None:

            @operation(publish=publish, security=security)
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

        elif isinstance(index, key_type):

            @operation(publish=publish, security=security)
            async def get(self) -> value_type:
                return await self[index].get()

        else:
            raise TypeError("unsupported index type")

        def __getitem__(self, key: key_type) -> FileResource:
            return FileResource(
                f"{_path}/{quote(codec.encode(key), safe='')}{extension if extension else ''}"
            )

    affix_type_hints(DirectoryResource, localns=locals())
    DirectoryResource.__qualname__ = "DirectoryResource"

    return DirectoryResource()
