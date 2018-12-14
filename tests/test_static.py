import roax.resource as r
import roax.schema as s
import unittest

from roax.static import StaticResource
from roax.resource import operation


_schema = s.bytes(format="binary")

_content = b"This is the content that will be returned."


class TestResource(StaticResource):
    
    def __init__(self):
        super().__init__(_content, _schema)


class TestStaticResource(unittest.TestCase):

    def test_read(self):
        tr = TestResource()
        self.assertEqual(tr.read(), _content)


if __name__ == "__main__":
    unittest.main()
