
import json
import os
import roax.schema as s
import tempfile
import unittest

from io import BytesIO
from roax.cli import CLI
from roax.resource import BadRequest, Resource, operation

class TestResource(Resource):

    @operation(params={"_body": s.bytes(format="binary")}, returns=s.dict({"id": s.str()}))
    def create(self, _body):
        if _body != b"hello_body":
            raise BadRequest("_body not hello_body")
        return {"id": "foo"}

    @operation(type="action", params={"a_a": s.int(), "b": s.str()}, returns=s.str())
    def foo(self, a_a, b):
        return "hoot"

cli = CLI(debug=False, err=None)
cli.register("test", TestResource())

class TestCLI(unittest.TestCase):

    def test_cli_params(self):
        line = "test foo --a-a=1 --b=abc"
        out = BytesIO()
        self.assertEqual(cli.process(line, out=out), True)
        self.assertEqual(out.getvalue(), b"hoot")

    def test_cli_create_binary_body_success(self):
        line = "test create"
        inp = BytesIO(b"hello_body")
        out = BytesIO()
        self.assertEqual(cli.process(line, inp=inp, out=out), True)
        out.seek(0)
        self.assertEqual(json.load(out), {"id": "foo"})

    def test_cli_create_binary_body_failure(self):
        line = "test create"
        inp = BytesIO(b"not_a_match")
        out = BytesIO()
        self.assertEqual(cli.process(line, inp=inp, out=out), False)

    def test_cli_redirect_in_out(self):
        with tempfile.NamedTemporaryFile() as inp:
            inp.write(b"hello_body")
            inp.flush()
            with tempfile.NamedTemporaryFile(delete=False) as out:
                out_name = out.name
                line = "test create <{} >{}".format(inp.name, out.name)
                self.assertEqual(cli.process(line, out=out), True)
            with open(out_name, "rb") as out:
                self.assertEqual(json.load(out), {"id": "foo"})
            os.remove(out_name)

if __name__ == "__main__":
    unittest.main()
