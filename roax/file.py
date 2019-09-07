"""Module to store resource items in files."""

import os
import os.path
import roax.resource
import roax.schema as s

try:
    import fcntl  # supported in *nix
except ImportError:
    fcntl = None


_map = [(c, "%{:02X}".format(ord(c))) for c in '%/\\:*?"<>|']


def _quote(s):
    """_quote('abc/def') -> 'abc%2Fdef'"""
    for m in _map:
        s = s.replace(m[0], m[1])
    return s


def _unquote(s):
    """_unquote('abc%2Fdef') -> 'abc/def'"""
    if "%" in s:
        for m in _map:
            s = s.replace(m[1], m[0])
    return s


class FileResource(roax.resource.Resource):
    """
    Base class for a file-based resource; each item is a stored as a separate file
    in a directory.

    Parameters and instance variables:
    • name: Short name of the resource. (default: the class name in lower case)
    • description: Short description of the resource. (default: the resource docstring)
    • dir: Directory to store resource items in.
    • schema: Schema for resource items.
    • id_schema: Schema for resource item identifiers. (default: str)
    • extenson: Filename extension to use for each file (including dot).

    This class is appropriate for up to hundreds or thousands of items; it is
    probably not appropriate for tens of thousands or more.
    """

    def __init__(
        self,
        dir=None,
        name=None,
        description=None,
        schema=None,
        id_schema=None,
        extension=None,
    ):
        super().__init__(name, description)
        self.dir = os.path.expanduser((dir or self.dir).rstrip("/"))
        self.schema = schema or self.schema
        self.id_schema = id_schema or getattr(self, "id_schema", s.str())
        self.extension = extension or getattr(self, "extension", "")
        os.makedirs(self.dir, exist_ok=True)
        self.__doc__ = self.schema.description

    def create(self, id, _body):
        """Create a resource item."""
        try:
            self._write("xb", id, _body)
        except roax.resource.NotFound:
            raise roax.resource.InternalServerError(
                f"{self.name} resource directory not found"
            )
        return {"id": id}

    def read(self, id):
        """Read a resource item."""
        return self._read("rb", id)

    def update(self, id, _body):
        """Update a resource item."""
        return self._write("wb", id, _body)

    def delete(self, id):
        """Delete a resource item."""
        try:
            os.remove(self._filename(id))
        except FileNotFoundError:
            raise roax.resource.NotFound(f"{self.name} item not found: {id}")

    def list(self):
        """Return a list of all resource item identifiers."""
        result = []
        try:
            listdir = os.listdir(self.dir)
        except FileNotFoundError:
            raise roax.resource.InternalServerError(
                f"{self.name} resource directory not found"
            )
        for name in listdir:
            if name.endswith(self.extension):
                name = name[: len(name) - len(self.extension)]
                str_id = _unquote(name)
                if name != _quote(str_id):  # ignore improperly encoded names
                    continue
                try:
                    result.append(self.id_schema.str_decode(str_id))
                except s.SchemaError:
                    pass  # ignore filenames that can't be parsed
        return result

    def _filename(self, id):
        """Return the full filename for the specified resource item identifier."""
        return f"{self.dir}/{_quote(self.id_schema.str_encode(id))}{self.extension}"

    def _open(self, id, mode):
        try:
            file = open(self._filename(id), mode)
            if fcntl:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            return file
        except FileNotFoundError:
            raise roax.resource.NotFound(f"{self.name} item not found: {id}")
        except FileExistsError:
            raise roax.resource.Conflict(f"{self.name} item already exists: {id}")

    def _read(self, mode, id):
        with self._open(id, mode) as file:
            try:
                result = self.schema.bin_decode(file.read())
                self.schema.validate(result)
            except s.SchemaError as se:
                raise roax.resource.InternalServerError(
                    "content read from file failed schema validation"
                ) from se
        return result

    def _write(self, mode, id, body):
        with self._open(id, mode) as file:
            file.write(self.schema.bin_encode(body))
