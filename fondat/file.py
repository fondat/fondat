"""Module to store resource items in files."""

import aiofiles
import dataclasses
import enum
import fondat.codec
import logging
import os
import os.path
import typing

from fondat.resource import resource, operation, InternalServerError, NotFound, In
from fondat.typing import affix_type_hints


_logger = logging.getLogger(__name__)


_map = {ord(c): "%{:02x}".format(ord(c)) for c in '.%/\\:*?"<>|'}


def _quote(s):
    """_quote('abc/def') -> 'abc%2fdef'"""
    return s.translate(_map)


def _unquote(s):
    """_unquote('abc%2fdef') -> 'abc/def'"""
    if "%" in s:
        for k, v in _map.items():
            s = s.replace(v, chr(k))
    return s


def file_resource(type_, compress=None, security=None):
    """
    Return a new resource class that manages content in a file.

    Parameters:
    • type_: Type of value stored in file.
    • compress: Algorithm to compress and decompress file content.
    • security: Security requirements to apply to file operations.

    Compression algorithm is any object or module that exposes callable
    "compress" and "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """

    codec = fondat.codec.get_codec(type_)

    @resource
    class FileResource:
        """..."""

        value_type = type_

        def __init__(self, path):
            self.path = path

        @operation(security=security)
        async def get(self) -> type_:
            """Read file."""
            try:
                async with aiofiles.open(self.path, "rb") as file:
                    content = await file.read()
                if compress:
                    content = compress.decompress(content)
                return codec.bytes_decode(content)
            except FileNotFoundError:
                raise NotFound
            except (TypeError, ValueError) as e:
                _logger.error(e)
                raise InternalServerError

        @operation(security=security)
        async def put(self, value: typing.Annotated[type_, In.BODY]):
            """Write file."""
            tmp = self.path + ".__tmp"
            content = codec.bytes_encode(value)
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
                raise NotFound

    affix_type_hints(FileResource, localns=locals())
    FileResource.__qualname__ = "FileResource"
    return FileResource


def directory_resource(
    path, key_type, value_type, extension=None, compress=None, security=None
):
    """
    Return a new resource class that manages files in a directory.

    Parameters:
    • path: Path to directory where files are stored.
    • key_type: Type of key to identify file.
    • value_type: Type of value stored in each file.
    • extenson: Filename extension to use for each file (including dot).
    • compress: Algorithm to compress and decompress file content.
    • security: Security requirements to apply to all operations.

    Compression algorithm is any object or module that exposes callable
    "compress" and "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """

    codec = fondat.codec.get_codec(key_type)

    if extension is None:
        extension = ""

    _path = os.path.expanduser((path).rstrip("/"))

    os.makedirs(_path, exist_ok=True)

    FileResource = file_resource(value_type, compress, security)

    @resource
    class DirectoryResource:
        @operation(security=security)
        async def get(self) -> list[key_type]:
            """Return a list of file keys."""
            try:
                listdir = os.listdir(_path)
            except FileNotFoundError:
                raise InternalServerError(f"directory not found: {_path}")
            keys = []
            for name in (n for n in listdir if n.endswith(extension)):
                if extension:
                    name = name[: -len(extension)]
                try:
                    keys.append(codec.str_decode(_unquote(name)))
                except ValueError:
                    continue  # ignore name that cannot be decoded
            return keys

        def __getitem__(self, key: key_type) -> FileResource:
            return FileResource(f"{_path}/{_quote(codec.str_encode(key))}{extension}")

    DirectoryResource.key_type = key_type
    DirectoryResource.value_type = value_type

    DirectoryResource.__qualname__ = "DirectoryResource"
    affix_type_hints(DirectoryResource, localns=locals())
    return DirectoryResource
