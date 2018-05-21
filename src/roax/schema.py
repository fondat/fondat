"""Module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax._schema as _schema

def _export(*args):
    for name in args:
        item = getattr(_schema, "_" + name, None) or getattr(_schema, name) 
        globals()[name] = item
        __all__.append(name)

__all__ = []

_export("SchemaError")

_export("type", "dict", "list", "set", "str", "int", "float", "bool", "bytes")
_export("datetime", "uuid")
_export("all_of", "any_of", "one_of")
_export("reader")

_export("call", "function_params", "validate")
