import fondat.context as context
import fondat.security
import pytest

from fondat.error import UnauthorizedError, ForbiddenError
from fondat.resource import resource, mutation


pytestmark = pytest.mark.asyncio


class Never(fondat.security.SecurityRequirement):
    def authorized(self):
        raise ForbiddenError


req1 = fondat.security.ContextSecurityRequirement(req1=True)

never = Never()


@resource
class R1:
    @mutation(security=[req1])
    async def foo(self) -> str:
        return "foo_success"

    @mutation(security=[req1, never])
    async def bar(self) -> str:
        return "bar_success"


async def test_security_req_success():
    r1 = R1()
    with context.push(req1=True):
        assert await r1.foo() == "foo_success"


async def test_security_req_forbidden():
    r1 = R1()
    with pytest.raises(UnauthorizedError):
        await r1.foo()
