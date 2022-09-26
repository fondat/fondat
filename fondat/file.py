"""File I/O module."""

import builtins
import logging
import mimetypes

from contextlib import suppress
from fondat.codec import Binary, DecodeError, String, get_codec
from fondat.error import ConflictError, InternalServerError, NotFoundError
from fondat.http import AsBody
from fondat.resource import operation, resource
from fondat.stream import Stream
from pathlib import Path
from typing import Annotated, Generic, TypeVar
from urllib.parse import quote, unquote


_logger = logging.getLogger(__name__)


K = TypeVar("K")
V = TypeVar("V")


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


class FileStream(Stream):
    """
    Represents a file as an asynchronous byte stream.

    Parameters:
    • path: ...
    • content_type: ...

    If content_type is not specified, this class will attempt to guess the content type
    from the filename extension.
    """

    def __init__(self, path: Path, content_type: str | None = None):
        super().__init__(
            content_type=content_type or _content_type(path.name),
            content_length=path.stat().st_size,
        )
        self.file = path.expanduser().open("rb")

    async def __anext__(self) -> bytes:
        if not self.file:
            raise StopAsyncIteration
        chunk = self.file.read1(1048576)  # 1 MiB
        if len(chunk) == 0:
            with suppress(Exception):
                self.file.close()
            self.file = None
            raise StopAsyncIteration
        return chunk

    def __del__(self):
        if self.file:
            with suppress(Exception):
                self.file.close()
        self.file = None


@resource
class DirectoryResource(Generic[K, V]):
    """
    Resource that represents files in a directory.

    Parameters:
    • path: path to directory where files are stored
    • key_type: type of key to identify each file
    • value_type: type of value stored in each file
    • extenson: filename extension to append (including dot)
    """

    def __init__(
        self,
        path: Path,
        key_type: type[K] = str,
        value_type: type[V] = Stream,
        extension: str | None = None,
    ):
        self.path = path.expanduser()
        if not self.path.is_dir():
            raise FileNotFoundError(f"directory not found: {self.path}")
        if not getattr(key_type, "__hash__", None):
            raise TypeError("invalid key_type: {key_type}")
        self.key_type = key_type
        self.value_type = value_type
        self.extension = extension
        self.key_codec = get_codec(String, key_type)

    @operation
    async def get(self) -> set[K]:
        """Return list of file keys."""
        try:
            keys = set()
            for name in (
                path.name[: -len(self.extension)] if self.extension else path.name
                for path in self.path.iterdir()
                if path.is_file()
                and not path.name.endswith(".__tmp__")
                and (not self.extension or path.name.endswith(self.extension))
            ):
                try:
                    keys.add(self.key_codec.decode(unquote(name)))
                except DecodeError:
                    pass  # ignore incompatible file names
            return keys
        except Exception as e:
            raise InternalServerError from e

    def __getitem__(self, key: K) -> "FileResource[V]":
        return FileResource(
            self.path.joinpath(
                quote(self.key_codec.encode(key), safe="") + (self.extension or "")
            ),
            self.value_type,
        )


@resource
class FileResource(Generic[V]):
    """
    Resource that represents a file.

    Parameters:
    • path: location of file
    • type: type of value stored in file
    """

    def __init__(
        self,
        path: Path,
        type: builtins.type[V] = Stream,
    ):
        self.path = path
        self.type = type
        self.codec = get_codec(Binary, type) if type is not Stream else None

    @operation
    async def get(self) -> V:
        """Read resource."""
        try:
            with self.path.open("rb") as file:
                if self.type is Stream:
                    return FileStream(self.path)
                else:
                    return self.codec.decode(file.read())
        except FileNotFoundError as fnfe:
            raise NotFoundError from fnfe
        except Exception as e:
            raise InternalServerError from e

    @operation
    async def put(self, value: Annotated[V, AsBody]):
        """Write resource."""
        try:
            tmp = self.path.with_name(f"{self.path.name}.__tmp__")
            with tmp.open("xb") as file:
                if self.type is Stream:
                    async for block in value:
                        file.write(block)
                else:
                    file.write(self.codec.encode(value))
            tmp.replace(self.path)
        except FileExistsError as e:
            raise ConflictError from e
        except Exception as e:
            raise InternalServerError from e

    @operation
    async def delete(self):
        """Delete resource."""
        try:
            self.path.unlink()
        except FileNotFoundError as fnfe:
            raise NotFoundError from fnfe
        except Exception as e:
            raise InternalServerError from e
