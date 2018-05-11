
import roax.schema as s
import unittest

from base64 import b64encode
from datetime import datetime
from roax.resource import Resource, Unauthorized, operation
from roax.security import SecurityRequirement, HTTPBasicSecurityScheme
from roax.wsgi import App
from webob import Request

class TestSecurityRequirement(SecurityRequirement):
    def __init__(self, scheme):
        self.scheme = scheme
    def authorize(self):
        ctx = self.scheme.get_context()
        if not ctx or ctx["role"] != "god":
            raise Unauthorized()

class TestSecurityScheme(HTTPBasicSecurityScheme):
    def authenticate(self, user_id, password):
        if user_id == "sparky" and password == "punkydoodle":
            return {"user_id": user_id, "role": "god"}

_scheme = TestSecurityScheme("WallyWorld")

http1 = TestSecurityRequirement(_scheme)

_r1_schema = s.dict({
    "id": s.str(),
    "foo": s.int(),
    "bar": s.bool(),
    "dt": s.datetime(required=False),
})

class _Resource1(Resource):
    
    schema = _r1_schema
    
    @operation(
        params = {"id": _r1_schema.properties["id"], "_body": _r1_schema},
        returns = s.dict({"id": _r1_schema.properties["id"]}),
        security = [],
    )
    def create(self, id, _body):
        return {"id": id}

    @operation(
        params = {"id": _r1_schema.properties["id"], "_body": _r1_schema},
        security = [],
    )
    def update(self, id, _body):
        return

    @operation(
        type = "action",
        params = {},
        returns = s.str(format="raw"),
        security = [http1]
    )
    def foo(self):
        return "foo_success"

    @operation(
        type = "action",
        params = {"uuid": s.uuid()},
        security = [http1],
    )
    def validate_uuid(self, uuid):
        pass

app = App("/", "Title", "1.0")
app.register("/r1", _Resource1())


class TestWSGI(unittest.TestCase):

    def test_create(self):
        request = Request.blank("/r1?id=id1")
        request.method = "POST"
        request.json = {"id": "id1", "foo": 1, "bar": True, "dt": _r1_schema.properties["dt"].json_encode(datetime.now())}
        response = request.get_response(app)
        result = response.json
        self.assertEqual(result, {"id": "id1"})
        self.assertEqual(response.status_code, 200)  # OK

    def test_update(self):
        request = Request.blank("/r1?id=id2")
        request.method = "PUT"
        request.json = {"id": "id2", "foo": 123, "bar": False}
        response = request.get_response(app)
        self.assertEqual(response.status_code, 204)  # No Content

    def test_http_req(self):
        request = Request.blank("/r1/foo")
        request.method = "POST"
        request.authorization = ("Basic", b64encode(b"sparky:punkydoodle").decode())
        response = request.get_response(app)
        self.assertEqual(response.status_code, 200)  # OK
        self.assertEqual(response.text, "foo_success")

    def test_http_validation_vs_auth_failure(self):
        request = Request.blank("/r1/validate_uuid?uuid=not-a-uuid")
        request.method = "POST"
        response = request.get_response(app)
        self.assertEqual(response.status_code, 401)  # authorization should trump validation

if __name__ == "__main__":
    unittest.main()
