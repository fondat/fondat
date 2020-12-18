"""Module for resource errors."""

import http

class Error(Exception):
    """Base class for errors."""
    pass


def _title(s):
    return s if s in {"HTTP", "URI"} else s.title()


# generate concrete error classes
for status in http.HTTPStatus:
    globalns = globals()
    if 400 <= status <= 599:
        name = "".join([_title(w) for w in status.name.split("_")])
        if not name.endswith("Error"):
            name += "Error"
        globalns[name] = type(name, (Error,), {"status": status})
