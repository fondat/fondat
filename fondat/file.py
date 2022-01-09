"""Module to store resource items in files."""

import logging
import mimetypes

from collections.abc import Iterable
from fondat.codec import Binary, String, get_codec
from fondat.error import InternalServerError, NotFoundError
from fondat.http import AsBody
from fondat.pagination import make_page_dataclass
from fondat.resource import resource, operation
from fondat.stream import Stream
from fondat.types import affix_type_hints
from fondat.security import Policy
from pathlib import Path
from typing import Annotated, Any, Optional, Union
from urllib.parse import quote, unquote


_logger = logging.getLogger(__name__)


_content_encoding_map = {
    "br": "application/x-br",
    "bzip2": "application/x-bzip2",
    "compress": "application/x-compress",
    "gzip": "application/gzip",
    "xz": "application/x-xz",
}


def _content_type(url: str) -> str:
    content_type, content_encoding = mimetypes.guess_type(url)
    if content_encoding:
        return _content_encoding_map.get(content_encoding, content_encoding)
    if content_type:
        return content_type
    return "application/octet-stream"


class _ReadFileStream(Stream):
    def __init__(self, path: Path, block_size: int = 131072):
        super().__init__(
            content_type=_content_type(path.name), content_length=path.stat().st_size
        )
        self.path = path
        self.block_size = block_size
        self.position = 0

    async def __anext__(self) -> bytes:
        if not self.path:
            raise StopAsyncIteration
        with self.path.open("rb") as file:
            file.seek(self.position)
            block = file.read1(self.block_size)
        if len(block) == 0:
            raise StopAsyncIteration
        self.position += len(block)
        return block


def _tmp_path(path: Path):
    return path.with_name(f"{path.name}.__tmp__")


def _stream_resource_class(
    writeable: bool,
    publish: bool,
    policies: Iterable[Policy],
):
    @resource
    class StreamResource:
        def __init__(self, path: Path):
            self.path = path

        @operation(publish=publish, policies=policies)
        async def get(self) -> Stream:
            """Read resource."""
            if not self.path.is_file():
                raise NotFoundError
            return _ReadFileStream(self.path)

        if writeable:

            @operation(publish=publish, policies=policies)
            async def put(self, value: Annotated[Stream, AsBody]):
                """Write resource."""
                tmp = _tmp_path(self.path)
                try:
                    with tmp.open("xb") as file:
                        async for block in value:
                            file.write(block)
                    tmp.replace(self.path)
                except Exception as e:
                    raise InternalServerError from e

            @operation(publish=publish, policies=policies)
            async def delete(self):
                """Delete resource."""
                try:
                    self.path.unlink()
                except FileNotFoundError:
                    raise NotFoundError
                except Exception as e:
                    raise InternalServerError from e

    affix_type_hints(StreamResource, localns=locals())
    StreamResource.__qualname__ = "StreamResource"
    return StreamResource


def _file_resource_class(
    value_type: type,
    compress: Any,
    writeable: bool,
    publish: bool,
    policies: Iterable[Policy],
):

    if value_type is Stream:
        if compress is not None:
            raise TypeError("file resources does not support compression of streams")
        return _stream_resource_class(writeable, publish, policies)

    codec = get_codec(Binary, value_type)

    @resource
    class FileResource:
        def __init__(self, path: Path):
            self.path = path

        @operation(publish=publish, policies=policies)
        async def get(self) -> value_type:
            """Read resource."""
            try:
                with self.path.open("rb") as file:
                    content = file.read()
                if compress:
                    content = compress.decompress(content)
                return codec.decode(content)
            except FileNotFoundError:
                raise NotFoundError
            except Exception as e:
                raise InternalServerError from e

        if writeable:

            @operation(publish=publish, policies=policies)
            async def put(self, value: Annotated[value_type, AsBody]):
                """Write resource."""
                content = codec.encode(value)
                if compress:
                    content = compress.compress(content)
                tmp = _tmp_path(self.path)
                try:
                    with tmp.open("xb") as file:
                        file.write(content)
                    tmp.replace(self.path)
                except Exception as e:
                    raise InternalServerError from e

            @operation(publish=publish, policies=policies)
            async def delete(self):
                """Delete resource."""
                try:
                    self.path.unlink()
                except FileNotFoundError:
                    raise NotFoundError

    affix_type_hints(FileResource, localns=locals())
    FileResource.__qualname__ = "FileResource"
    return FileResource


def file_resource(
    path: Union[Path, str],
    value_type: type = Stream,
    compress: Any = None,
    writeable: bool = False,
    publish: bool = True,
    policies: Optional[Iterable[Policy]] = None,
) -> type:
    """
    Return a new resource that manages a file.

    Parameters:
    • path: path of file
    • value_type: type of value stored in file
    • compress: algorithm to compress and decompress file content
    • writeable: can file be written or deleted
    • publish: publish the operation in documentation
    • policies: security policies to apply to file operations

    Compression algorithm is any object or module that exposes callable "compress" and
    "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """
    if isinstance(path, str):
        path = Path(path)
    return _file_resource_class(value_type, compress, writeable, publish, policies)(path)


def _limit(requested):
    upper = 1000
    if not requested or requested < 0:
        return upper
    return min(requested, upper)


def directory_resource(
    path: Union[Path, str],
    key_type: type = str,
    value_type: type = Stream,
    extension: Optional[str] = None,
    compress: Any = None,
    writeable: bool = False,
    index: bool = True,
    publish: bool = True,
    policies: Optional[Iterable[Policy]] = None,
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
    • index: provide index of files with get method
    • publish: publish the operation in documentation
    • policies: Security requirements to apply to all operations

    Compression algorithm is any object or module that exposes callable "compress" and
    "decompress" attributes. Examples: bz2, gzip, lzma, zlib. Compression is not supported for
    value_type of Stream.

    The index parameter can be one of the following:
    • key_type: identifies a file in the directory to provide as the index
    • None: generates a paginated list of file keys as the index
    """

    _path = (Path(path) if isinstance(path, str) else path).expanduser()

    if not _path.is_dir():
        raise FileNotFoundError(f"directory not found: {_path}")

    codec = get_codec(String, key_type)

    Page = make_page_dataclass("Page", key_type)
    FileResource = _file_resource_class(value_type, compress, writeable, publish, policies)

    @resource
    class DirectoryResource:
        def __getitem__(self, key: key_type) -> FileResource:
            return FileResource(
                _path.joinpath(
                    f"{quote(codec.encode(key), safe='')}{extension if extension else ''}"
                )
            )

        if index:

            @operation(publish=publish, policies=policies)
            async def get(
                self, limit: Optional[int] = None, cursor: Optional[bytes] = None
            ) -> Page:
                """Return paginated list of file keys."""
                limit = _limit(limit)
                if cursor is not None:
                    cursor = cursor.decode()
                try:
                    if not extension:
                        names = sorted(
                            entry.name for entry in _path.iterdir() if entry.is_file()
                        )
                    else:
                        names = sorted(
                            entry.name[: -len(extension)]
                            for entry in _path.iterdir()
                            if entry.is_file() and entry.name.endswith(extension)
                        )
                except FileNotFoundError as fnfe:
                    raise InternalServerError from fnfe
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

    affix_type_hints(DirectoryResource, localns=locals())
    DirectoryResource.__qualname__ = "DirectoryResource"

    return DirectoryResource()
