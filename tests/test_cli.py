
import json
import roax.schema as s
import unittest

from io import BytesIO, StringIO
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
        out = StringIO()
        self.assertEqual(cli.process(line, out=out), True)
        self.assertEqual(out.getvalue(), "hoot")

    def test_cli_create_binary_body_success(self):
        line = "test create"
        inp = BytesIO(b"hello_body")
        out = StringIO()
        self.assertEqual(cli.process(line, inp=inp, out=out), True)
        self.assertEqual(json.loads(out.getvalue()), {"id": "foo"})

    def test_cli_create_binary_body_failure(self):
        line = "test create"
        inp = BytesIO(b"not_a_match")
        out = StringIO()
        self.assertEqual(cli.process(line, inp=inp, out=out), False)

if __name__ == "__main__":
    unittest.main()
