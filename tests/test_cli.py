import json
import os
import pytest
import roax.schema as s
import tempfile

from io import BytesIO
from roax.cli import CLI
from roax.resource import Resource, operation


class _TestResource(Resource):
    @operation(
        params={"_body": s.bytes(format="binary")}, returns=s.dict({"id": s.str()})
    )
    def create(self, _body):
        if _body != b"hello_body":
            raise BadRequest("_body not hello_body")
        return {"id": "foo"}

    @operation(type="action", params={"a_a": s.int(), "b": s.str()}, returns=s.str())
    def foo(self, a_a, b):
        return "hoot"

    @operation(type="action", params={"_body": s.reader()}, returns=s.reader())
    def echo(self, _body):
        return BytesIO(_body.read())


@pytest.fixture(scope="module")
def cli():
    cli = CLI(debug=False, err=None)
    cli.register_resource("test", _TestResource())
    return cli


def test_cli_params(cli):
    line = "test foo --a-a=1 --b=abc"
    out = BytesIO()
    assert cli.process(line, out=out) == True
    assert out.getvalue() == b"hoot"


def test_cli_create_binary_body_success(cli):
    line = "test create"
    inp = BytesIO(b"hello_body")
    out = BytesIO()
    assert cli.process(line, inp=inp, out=out) == True
    out.seek(0)
    assert json.loads(out.getvalue().decode()) == {"id": "foo"}


def test_cli_create_binary_body_failure(cli):
    line = "test create"
    inp = BytesIO(b"not_a_match")
    out = BytesIO()
    assert cli.process(line, inp=inp, out=out) == False


def test_cli_redirect_in_out(cli):
    with tempfile.NamedTemporaryFile() as inp:
        inp.write(b"hello_body")
        inp.flush()
        with tempfile.NamedTemporaryFile(delete=False) as out:
            out_name = out.name
            line = "test create <{} >{}".format(inp.name, out.name)
            assert cli.process(line, out=out) == True
        with open(out_name, "rb") as out:
            assert json.loads(out.read().decode()) == {"id": "foo"}
        os.remove(out_name)


def test_cli_reader_in_out(cli):
    value = b"This is a value to be tested."
    with tempfile.NamedTemporaryFile() as inp:
        inp.write(value)
        inp.seek(0)
        with tempfile.NamedTemporaryFile() as out:
            line = "test echo <{} >{}".format(inp.name, out.name)
            cli.process(line)
            out.seek(0)
            assert out.read() == value


def test_cli_help(cli):
    assert cli.process("help") == False
    assert cli.process("help test") == False
    assert cli.process("help test foo") == False
    assert cli.process("help help") == False
