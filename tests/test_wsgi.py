import base64
import datetime
import io
import pytest
import roax.context as context
import roax.security
import roax.schema as s
import roax.wsgi
import tempfile
import webob

from roax.resource import Resource, operation


class _TestBasicSecurityRequirement(roax.security.SecurityRequirement):
    def __init__(self, scheme):
        self.scheme = scheme

    def authorize(self):
        ctx = context.last(context="auth")
        if not ctx or ctx["role"] != "god":
            raise roax.resource.Unauthorized


class _TestBasicSecurityScheme(roax.wsgi.HTTPBasicSecurityScheme):
    def authenticate(self, user_id, password):
        if user_id == "sparky" and password == "punkydoodle":
            return {
                "context": "auth",
                "type": "basic",
                "scheme": self.scheme,
                "realm": self.realm,
                "user_id": user_id,
                "role": "god",
            }


_scheme = _TestBasicSecurityScheme("WallyWorld")

http1 = _TestBasicSecurityRequirement(_scheme)

_r1_schema = s.dict(
    {"id": s.str(), "foo": s.int(), "bar": s.bool(), "dt": s.datetime()}, "id foo bar"
)


class _BasicResource1(Resource):

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
        return io.BytesIO(_body.read())

    @operation(type="query", security=[])
    def optional(self, optional: s.str() = "default") -> s.str():
        return optional


app1 = roax.wsgi.App("/", "Title", "1.0")
app1.register_resource("/r1", _BasicResource1())


def test_create():
    request = webob.Request.blank("/r1?id=id1")
    request.method = "POST"
    request.json = {
        "id": "id1",
        "foo": 1,
        "bar": True,
        "dt": _r1_schema.props["dt"].json_encode(datetime.datetime.now()),
    }
    response = request.get_response(app1)
    result = response.json
    assert result == {"id": "id1"}
    assert response.status_code == 200  # OK


def test_update():
    request = webob.Request.blank("/r1?id=id2")
    request.method = "PUT"
    request.json = {"id": "id2", "foo": 123, "bar": False}
    response = request.get_response(app1)
    assert response.status_code == 204  # No Content


def test_http_req():
    request = webob.Request.blank("/r1/foo")
    request.method = "POST"
    request.authorization = ("Basic", base64.b64encode(b"sparky:punkydoodle").decode())
    response = request.get_response(app1)
    assert response.status_code == 200  # OK
    assert response.text == "foo_success"


def test_http_validation_vs_auth_failure():
    request = webob.Request.blank("/r1/validate_uuid?uuid=not-a-uuid")
    request.method = "POST"
    response = request.get_response(app1)
    assert response.status_code == 401  # authorization should trump validation


def test_echo():
    value = b"This is an echo test."
    request = webob.Request.blank("/r1/echo")
    request.method = "POST"
    request.body = value
    response = request.get_response(app1)
    assert response.body == value


def test_static_dir():
    foo = "<html><body>Foo</body></html>"
    bar = b"binary"
    with tempfile.TemporaryDirectory() as td:
        with open(f"{td}/foo.html", "w") as f:
            f.write(foo)
        with open(f"{td}/bar.bin", "wb") as f:
            f.write(bar)
        a = roax.wsgi.App("/", "Title", "1.0")
        a.register_static("/static", td, [])
        response = webob.Request.blank("/static/foo.html").get_response(a)
        assert response.body == foo.encode()
        response = webob.Request.blank("/static/bar.bin").get_response(a)
        assert response.body == bar
        assert response.content_type == "application/octet-stream"


def test_static_dir_index():
    index = "<html><body>Index</body></html>"
    with tempfile.TemporaryDirectory() as td:
        with open("{}/index.html".format(td), "w") as f:
            f.write(index)
        a = roax.wsgi.App("/", "Title", "1.0")
        a.register_static("/static", td, [])
        for path in ["/static/", "/static/index.html"]:
            request = webob.Request.blank(path)
            response = request.get_response(a)
            assert response.body == index.encode()
            assert response.content_type == "text/html"


def test_static_file():
    bar = b"binary"
    with tempfile.TemporaryDirectory() as td:
        filename = "{}/bar.bin".format(td)
        with open("{}/bar.bin".format(td), "wb") as f:
            f.write(bar)
        a = roax.wsgi.App("/", "Title", "1.0")
        a.register_static(filename, filename, [])
        request = webob.Request.blank(filename)
        response = request.get_response(a)
        assert response.body == bar


def test_optional_omit():
    request = webob.Request.blank("/r1/optional")
    request.method = "GET"
    response = request.get_response(app1)
    assert response.status_code == 200
    assert response.body.decode() == "default"


def test_optional_submit():
    request = webob.Request.blank("/r1/optional?optional=foo")
    request.method = "GET"
    response = request.get_response(app1)
    assert response.status_code == 200
    assert response.body.decode() == "foo"


class _TestCookieScheme(roax.wsgi.CookieSecurityScheme):
    def __init__(self):
        super().__init__("my_scheme", "token")

    def authenticate(self, value):
        if value == "super":
            return {"context": "auth", "user_id": value, "role": "superuser"}


class _TestCookieSecurityRequirement(roax.security.SecurityRequirement):
    def __init__(self):
        self.scheme = _TestCookieScheme()

    def authorize(self):
        ctx = context.last(context="auth")
        if not ctx:
            raise roax.resource.Unauthorized


cookie = _TestCookieSecurityRequirement()


class _CookieResource(Resource):
    @operation(security=[cookie])
    def read(self) -> s.str():
        return "success"


app2 = roax.wsgi.App("/", "Title", "1.0")
app2.register_resource("/r2", _CookieResource())


def test_cookie_unauthorized():
    request = webob.Request.blank("/r2")
    request.method = "GET"
    response = request.get_response(app2)
    assert response.status_code == 401


def test_cookie_success():
    request = webob.Request.blank("/r2")
    request.method = "GET"
    request.cookies["token"] = "super"
    response = request.get_response(app2)
    assert response.status_code == 200
    assert response.body.decode() == "success"
