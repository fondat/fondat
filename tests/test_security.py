
import roax.schema as s
import unittest

from roax.context import context, get_context
from roax.resource import Resource, Unauthorized, operation
from roax.security import SecurityRequirement

class Req1(SecurityRequirement):
    def authorize(self):
        if not get_context(req1=True):
            raise Unauthorized()

class Never(SecurityRequirement):
    def authorized(self):
        raise Unauthorized()

req1 = Req1()

never = Never()

class R1(Resource):
    
    @operation(
        type = "action",
        params = {},
        returns = s.str(),
        security = [req1],
    )
    def foo(self):
        return "foo_success"

    @operation(
        type = "action",
        params = {},
        returns = s.str(),
        security = [req1, never],
    )
    def bar(self):
        return "bar_success"

class TestSecurity(unittest.TestCase):

    def test_security_req_success(self):
        r1 = R1()
        with context(req1=True):
            self.assertEqual(r1.foo(), "foo_success")

    def test_security_req_unauth(self):
        r1 = R1()
        with self.assertRaises(Unauthorized):
            r1.foo()

if __name__ == "__main__":
    unittest.main()
