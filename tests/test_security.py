import pytest
import roax.context as context
import roax.schema as s

from roax.resource import Forbidden, Resource, operation
from roax.security import ContextSecurityRequirement, SecurityRequirement, nested


class Never(SecurityRequirement):
    def authorized(self):
        raise Forbidden


req1 = ContextSecurityRequirement(req1=True)

never = Never()


class R1(Resource):
    @operation(type="action", security=[req1])
    def foo(self) -> s.str():
        return "foo_success"

    @operation(type="action", security=[req1, never])
    def bar(self) -> s.str():
        return "bar_success"

    @operation(type="action", security=[nested])
    def nestee(self) -> s.str():
        return "nest_success"

    @operation(type="action")
    def nester(self) -> s.str():
        return self.nestee()


class R2(Resource):

    security = [req1]

    @operation(type="action")
    def foo(self) -> s.str():
        return "foo_success"


def test_security_req_success():
    r1 = R1()
    with context.push(req1=True):
        assert r1.foo() == "foo_success"


def test_security_req_forbidden():
    r1 = R1()
    with pytest.raises(Forbidden):
        r1.foo()


def test_security_req_multiple_unnested():
    r1 = R1()
    for n in range(0, 3):
        with pytest.raises(Forbidden):
            r1.nestee()


def test_security_req_nested():
    r1 = R1()
    assert r1.nester() == "nest_success"


def test_resource_security_success():
    r2 = R2()
    with context.push(req1=True):
        assert r2.foo() == "foo_success"


def test_resource_security_forbidden():
    r2 = R2()
    with pytest.raises(Forbidden):
        r2.foo()
