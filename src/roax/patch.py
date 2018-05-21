"""Implementations of patch operations."""

def merge_patch(self, value, doc):
    """
    Modify a dictionary with a JSON Marge Patch document, per RFC 7396.

    :param value: Dictionary value to be patched.
    :param doc: JSON Merge Patch document to modify dictionary value. 
    """
    if not isinstance(value, Mapping):
        raise InternalServerError("can only patch dict items")
    for k, v in doc.items():
        if v is None and k in value:
            del value[k]
        elif v is not None:
            value[k] = v
