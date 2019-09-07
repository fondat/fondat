"""Module to define, encode, decode and validate JSON data structures."""

import roax._schema as _schema


def _export(*args):
    for name in args:
        globals()[name] = getattr(_schema, "_" + name, None) or getattr(_schema, name)


_export(
    "type",
    "dict",
    "list",
    "set",
    "str",
    "int",
    "float",
    "bool",
    "bytes",
    "date",
    "datetime",
    "uuid",
    "all_of",
    "any_of",
    "one_of",
    "reader",
    "call",
    "function_params",
    "validate",
    "SchemaError",
)
