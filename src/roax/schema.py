"""Module to define, encode, decode and validate JSON data structures."""

import roax._schema as _schema

def _export(*args):
    for name in args:
        item = getattr(_schema, "_" + name, None) or getattr(_schema, name) 
        globals()[name] = item
        __all__.append(name)

__all__ = []

_export("SchemaError")

_export("type", "dict", "list", "set", "str", "int", "float", "bool", "bytes")
_export("date", "datetime", "uuid")
_export("all_of", "any_of", "one_of")
_export("reader")

_export("call", "function_params", "validate")
