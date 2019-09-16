import pytest
import roax.context as context
import roax.schema as s

from base64 import b64encode
from datetime import datetime
from io import BytesIO
from roax.resource import Resource, Unauthorized, operation
from roax.security import SecurityRequirement
from roax.wsgi import App, HTTPBasicSecurityScheme
from tempfile import TemporaryDirectory
from webob import Request


class _TestSecurityRequirement(SecurityRequirement):
    def __init__(self, scheme):
        self.scheme = scheme

    def authorize(self):
        ctx = context.last(context="auth")
        if not ctx or ctx["role"] != "god":
            raise Unauthorized


class _TestSecurityScheme(HTTPBasicSecurityScheme):
    def authenticate(self, user_id, password):
        if user_id == "sparky" and password == "punkydoodle":
            return {"user_id": user_id, "role": "god"}


_scheme = _TestSecurityScheme("WallyWorld")

http1 = _TestSecurityRequirement(_scheme)

_r1_schema = s.dict(
    {"id": s.str(), "foo": s.int(), "bar": s.bool(), "dt": s.datetime()}, "id foo bar"
)


class _Resource1(Resource):

    schema = _r1_schema

    @operation(security=[])
    def create(
        self, id: _r1_schema.props["id"], _body: _r1_schema
    ) -> s.dict({"id": _r1_schema.props["id"]}):
        return {"id": id}

    @operation(security=[])
    def update(self, id: _r1_schema.props["id"], _body: _r1_schema):
        return

    @operation(type="action", security=[http1])
    def foo(self) -> s.str(format="raw"):
        return "foo_success"

    @operation(type="action", security=[http1])
    def validate_uuid(self, uuid: s.uuid()):
        pass

    @operation(type="action", security=[])
    def echo(self, _body: s.reader()) -> s.reader():
        return BytesIO(_body.read())

    @operation(type="query", security=[])
    def optional(self, optional: s.str() = "default") -> s.str():
        return optional


app = App("/", "Title", "1.0")
app.register_resource("/r1", _Resource1())


def test_create():
    request = Request.blank("/r1?id=id1")
    request.method = "POST"
    request.json = {
        "id": "id1",
        "foo": 1,
        "bar": True,
        "dt": _r1_schema.props["dt"].json_encode(datetime.now()),
    }
    response = request.get_response(app)
    result = response.json
    assert result == {"id": "id1"}
    assert response.status_code == 200  # OK


def test_update():
    request = Request.blank("/r1?id=id2")
    request.method = "PUT"
    request.json = {"id": "id2", "foo": 123, "bar": False}
    response = request.get_response(app)
    assert response.status_code == 204  # No Content


def test_http_req():
    request = Request.blank("/r1/foo")
    request.method = "POST"
    request.authorization = ("Basic", b64encode(b"sparky:punkydoodle").decode())
    response = request.get_response(app)
    assert response.status_code == 200  # OK
    assert response.text == "foo_success"


def test_http_validation_vs_auth_failure():
    request = Request.blank("/r1/validate_uuid?uuid=not-a-uuid")
    request.method = "POST"
    response = request.get_response(app)
    assert response.status_code == 401  # authorization should trump validation


def test_echo():
    value = b"This is an echo test."
    request = Request.blank("/r1/echo")
    request.method = "POST"
    request.body = value
    response = request.get_response(app)
    assert response.body == value


def test_static_dir():
    foo = "<html><body>Foo</body></html>"
    bar = b"binary"
    with TemporaryDirectory() as td:
        with open(f"{td}/foo.html", "w") as f:
            f.write(foo)
        with open(f"{td}/bar.bin", "wb") as f:
            f.write(bar)
        a = App("/", "Title", "1.0")
        a.register_static("/static", td, [])
        response = Request.blank("/static/foo.html").get_response(a)
        assert response.body == foo.encode()
        response = Request.blank("/static/bar.bin").get_response(a)
        assert response.body == bar
        assert response.content_type == "application/octet-stream"


def test_static_dir_index():
    index = "<html><body>Index</body></html>"
    with TemporaryDirectory() as td:
        with open("{}/index.html".format(td), "w") as f:
            f.write(index)
        a = App("/", "Title", "1.0")
        a.register_static("/static", td, [])
        for path in ["/static/", "/static/index.html"]:
            request = Request.blank(path)
            response = request.get_response(a)
            assert response.body == index.encode()
            assert response.content_type == "text/html"


def test_static_file():
    bar = b"binary"
    with TemporaryDirectory() as td:
        filename = "{}/bar.bin".format(td)
        with open("{}/bar.bin".format(td), "wb") as f:
            f.write(bar)
        a = App("/", "Title", "1.0")
        a.register_static(filename, filename, [])
        request = Request.blank(filename)
        response = request.get_response(a)
        assert response.body == bar


def test_optional_omit():
    request = Request.blank("/r1/optional")
    request.method = "GET"
    response = request.get_response(app)
    assert response.status_code == 200
    assert response.body.decode() == "default"


def test_optional_submit():
    request = Request.blank("/r1/optional?optional=foo")
    request.method = "GET"
    response = request.get_response(app)
    assert response.status_code == 200
    assert response.body.decode() == "foo"
