import fondat.context as context
import fondat.schema as s
import fondat.security
import pytest

from fondat.resource import resource, operation


pytestmark = pytest.mark.asyncio


class Never(fondat.security.SecurityRequirement):
    def authorized(self):
        raise Forbidden


req1 = fondat.security.ContextSecurityRequirement(req1=True)

never = Never()


@resource
class R1:
    @operation(type="mutation", security=[req1])
    async def foo(self) -> s.str():
        return "foo_success"

    @operation(type="mutation", security=[req1, never])
    async def bar(self) -> s.str():
        return "bar_success"


async def test_security_req_success():
    r1 = R1()
    with context.push(req1=True):
        assert await r1.foo() == "foo_success"


async def test_security_req_forbidden():
    r1 = R1()
    with pytest.raises(fondat.resource.Unauthorized):
        await r1.foo()
