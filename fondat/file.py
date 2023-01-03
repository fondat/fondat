"""File I/O module."""

import fondat.error
import logging
import mimetypes

from contextlib import suppress
from fondat.codec import BinaryCodec, DecodeError, StringCodec
from fondat.http import AsBody
from fondat.resource import operation, resource
from fondat.stream import Reader, Stream
from pathlib import Path
from typing import Annotated, BinaryIO, Generic, TypeVar
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
    Streams the content of a file through an asynchronous byte stream.

    Parameters:
    • path: path to the file to read
    • content_type: content type of the file, or None if unknown

    If content_type is not specified, this class will attempt to guess the content type
    from the filename extension.
    """

    def __init__(self, path: Path, content_type: str | None = None):
        self.file = path.open("rb")
        self.file.seek(0, 2)
        if content_type is None:
            content_type = _content_type(path.name)
        content_length = self.file.tell()
        self.file.seek(0, 0)
        super().__init__(content_type, content_length)

    async def __anext__(self) -> bytes:
        if self.file:
            chunk = self.file.read1(1048576)  # 1 MiB
            if len(chunk):
                return chunk
            await self.close()
        raise StopAsyncIteration

    async def close(self) -> None:
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
    • writable: allow files to be written and deleted
    """

    def __init__(
        self,
        path: Path,
        key_type: type[K] = str,
        value_type: type[V] = Stream,
        extension: str | None = None,
        writable: bool = False,
    ):
        self._path = path.expanduser()
        if not self._path.is_dir():
            raise FileNotFoundError(f"directory not found: {self._path}")
        if not getattr(key_type, "__hash__", None):
            raise TypeError("invalid key_type: {key_type}")
        self._key_type = key_type
        self._value_type = value_type
        self._extension = extension
        self._key_codec = StringCodec.get(key_type)
        self._writable = writable

    @operation(publish=False)
    async def get(self) -> list[K]:
        """Return list of file keys."""
        try:
            keys = []
            for name in (
                path.name[: -len(self._extension)] if self._extension else path.name
                for path in self._path.iterdir()
                if path.is_file()
                and not path.name.endswith(".__tmp__")
                and (not self._extension or path.name.endswith(self._extension))
            ):
                try:
                    keys.append(self._key_codec.decode(unquote(name)))
                except DecodeError:
                    pass  # ignore incompatible file names
            return keys
        except Exception as e:
            raise fondat.error.InternalServerError from e

    def __getitem__(self, key: K) -> "FileResource[V]":
        return FileResource(
            self._path.joinpath(
                quote(self._key_codec.encode(key), safe="") + (self._extension or "")
            ),
            self._value_type,
            self._writable,
        )


@resource
class FileResource(Generic[V]):
    """
    Resource that represents a file.

    Parameters:
    • path: location of file
    • type: type of value stored in file
    • writable: allow file to be written and deleted
    """

    def __init__(
        self,
        path: Path,
        type: type[V] = Stream,
        writable: bool = False,
    ):
        self._path = path.expanduser()
        self._type = type
        self._codec = BinaryCodec.get(type) if type is not Stream else None
        self._writable = writable

    @operation(publish=False)
    async def get(self) -> V:
        """Read resource."""
        try:
            stream = FileStream(self._path)
            if self._type is Stream:
                return stream
            async with stream:  # close stream after reading
                return self._codec.decode(await Reader(stream).read())
        except FileNotFoundError as fnfe:
            raise fondat.error.NotFoundError from fnfe
        except Exception as e:
            raise fondat.error.InternalServerError from e

    @operation(publish=False)
    async def put(self, value: Annotated[V, AsBody]) -> None:
        """Write resource."""
        if not self._writable:
            raise fondat.error.MethodNotAllowedError
        try:
            tmp = self._path.with_name(f"{self._path.name}.__tmp__")
            with tmp.open("xb") as file:
                if self._type is Stream:
                    async with value as stream:  # close stream after reading
                        await write_stream(stream, file)
                else:
                    file.write(self._codec.encode(value))
            tmp.replace(self._path)
        except FileExistsError as e:
            raise fondat.error.ConflictError from e
        except Exception as e:
            raise fondat.error.InternalServerError from e

    @operation(publish=False)
    async def delete(self) -> None:
        """Delete resource."""
        if not self._writable:
            raise fondat.error.MethodNotAllowedError
        try:
            self._path.unlink()
        except FileNotFoundError as fnfe:
            raise fondat.error.NotFoundError from fnfe
        except Exception as e:
            raise fondat.error.InternalServerError from e


async def write_stream(stream: Stream, file: BinaryIO) -> None:
    """Write a stream to a binary file-like object."""
    async for chunk in stream:
        file.write(chunk)
