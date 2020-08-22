"""Module to store resource items in files."""

import aiofiles
import dataclasses
import enum
import fondat.resource
import fondat.schema as s
import logging
import os
import os.path

from fondat.resource import resource, operation


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


def file_resource(path, schema, extension="", compress=None, security=None):
    """
    Return a resource class that manages files in a directory.

    Parameters:
    • path: Path to directory where files are stored.
    • schema: Schema of file content.
    • extenson: Filename extension to use for each file (including dot).
    • compress: Algorithm to compress and decompress file content.
    • security: Security requirements to apply to all operations.

    Compression algorithm is any object or module that exposes callable
    "compress" and "decompress" attributes. Examples: bz2, gzip, lzma, zlib.
    """

    _path = os.path.expanduser((path).rstrip("/"))

    os.makedirs(_path, exist_ok=True)

    def filename(id):
        return f"{_path}/{_quote(id)}{extension}"

    async def read(file):
        async with aiofiles.open(file, "rb") as file:
            content = await file.read()
            if compress:
                content = compress.decompress(content)
            return schema.bin_decode(content)

    async def write(file, content):
        if compress:
            content = compress.compress(content)
        async with aiofiles.open(file, "xb") as file:
            await file.write(content)

    @resource
    class FileResource:
        @operation(security=security)
        async def get(self, id: s.str()) -> schema:
            """Read item."""
            try:
                result = await read(filename(id))
            except FileNotFoundError:
                raise fondat.resource.NotFound(f"item not found: {id}")
            except (TypeError, s.SchemaError) as e:
                _logger.error(e)
                raise fondat.resource.InternalServerError
            return result

        @operation(security=security)
        async def put(self, id: s.str(), data: schema):
            """Write item."""
            dst = filename(id)
            tmp = dst + ".__tmp"
            content = schema.bin_encode(data)
            try:
                await write(tmp, content)
            except FileNotFoundError:
                raise fondat.resource.InternalServerError(
                    "resource directory not found"
                )
            os.replace(tmp, dst)

        @operation(security=security)
        async def delete(self, id: s.str()):
            """Delete item."""
            try:
                os.remove(filename(id))
            except FileNotFoundError:
                raise fondat.resource.NotFound(f"item not found: {id}")

        @operation(type="query", security=security)
        async def list(self) -> s.list(s.str()):
            """Return list of item identifiers."""
            result = []
            try:
                listdir = os.listdir(_path)
            except FileNotFoundError:
                raise fondat.resource.InternalServerError(
                    "resource directory not found"
                )
            for name in filter(lambda n: n.endswith(extension), listdir):
                if extension:
                    name = name[: -len(extension)]
                id = _unquote(name)
                if _quote(id) != name:  # ignore improperly encoded names
                    continue
                result.append(id)
            return result

    return FileResource
