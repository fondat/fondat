"""Module to define, encode, decode and validate JSON data structures."""

import fondat._schema as _schema


__all__ = []


def _export(*args):
    glob = globals()
    for name in args:
        obj = getattr(_schema, "_" + name, None) or getattr(_schema, name)
        obj.__name__ = name
        obj.__module__ = __name__
        glob[name] = obj
        __all__.append(name)


_export(
    "SchemaError",
    "type",
    "dict",
    "list",
    "set",
    "str",
    "int",
    "float",
    "decimal",
    "bool",
    "bytes",
    "date",
    "datetime",
    "uuid",
    "all_of",
    "any_of",
    "one_of",
    "reader",
    "dataclass",
    "resource",
    "validate",
    "validate_arguments",
    "validate_return",
    "data",
)
