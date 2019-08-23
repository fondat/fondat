import pytest
import roax.resource as r
import roax.schema as s

from roax.static import StaticResource
from roax.resource import operation


_schema = s.bytes(format="binary")

_content = b"This is the content that will be returned."


class _TestResource(StaticResource):
    def __init__(self):
        super().__init__(_content, _schema)


def test_read():
    tr = _TestResource()
    assert tr.read() == _content
