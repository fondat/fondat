"""Module for resource errors."""

import http


class Error(Exception):
    """
    Base class for errors.

    Each error class must expose the following attributes:
    • status: HTTP status code (integer)
    • phrase: HTTP reason phrase
    """


def _generate_errors():
    """Generate concrete error classes from HTTP statuses."""

    def _title(s):
        return s if s in {"HTTP", "URI"} else s.title()

    for status in http.HTTPStatus:
        globalns = globals()
        if 400 <= status <= 599:
            name = "".join([_title(w) for w in status.name.split("_")])
            if not name.endswith("Error"):
                name += "Error"
            doc = status.description or status.phrase.capitalize()
            if not doc.endswith("."):
                doc += "."
            globalns[name] = type(
                name,
                (Error,),
                {
                    "status": status.value,
                    "phrase": status.phrase,
                    "__doc__": doc,
                },
            )


_generate_errors()
