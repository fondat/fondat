"""Module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from roax._schema import _type as type
from roax._schema import _dict as dict
from roax._schema import _list as list
from roax._schema import _str as str
from roax._schema import _int as int
from roax._schema import _float as float
from roax._schema import _bool as bool
from roax._schema import _bytes as bytes
from roax._schema import _none as none
from roax._schema import _datetime as datetime
from roax._schema import _uuid as uuid
from roax._schema import _all as all
from roax._schema import _any as any
from roax._schema import _one as one

from roax._schema import call
from roax._schema import validate
from roax._schema import SchemaError

__all__ = [ "type", "dict", "list", "str", "int", "float", "bool",
        "bytes", "none", "datetime", "uuid", "all", "any", "one",
        "call", "validate", "SchemaError" ]
