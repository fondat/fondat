import pytest
import roax.schema as s

from roax.openapi import OpenAPIResource
from roax.resource import Resource, operation
from roax.wsgi import App


class _TestResource(Resource):
    @operation(type="action", security=[])
    def test(
        self,
        a: s.list(s.str()),
        b: s.set(s.str()),
        c: s.int(),
        d: s.float(),
        e: s.bool(),
        f: s.bytes(),
        g: s.datetime(),
        h: s.uuid(),
    ):
        pass


app = App("/", "Title", "1.0")
app.register_resource("/test", _TestResource())


def test_openapi_no_errors():
    resource = OpenAPIResource(app)
    resource.read()
