import pytest

from fondat.error import ForbiddenError, UnauthorizedError
from fondat.resource import mutation, resource
from fondat.security import Policy


async def authorized_rule():
    pass


authorized_policy = Policy(rules=[authorized_rule])


async def forbidden_rule():
    raise ForbiddenError


forbidden_policy = Policy(rules=[forbidden_rule])


async def unauthorized_rule():
    raise UnauthorizedError


unauthorized_policy = Policy(rules=[unauthorized_rule])


@resource
class R1:
    @mutation
    async def none(self) -> str:
        return "none"

    @mutation(policies=[])
    async def empty(self) -> str:
        return "empty"

    @mutation(policies=[authorized_policy])
    async def authorized(self) -> str:
        return "authorized"

    @mutation(policies=[unauthorized_policy, forbidden_policy, authorized_policy])
    async def authorized_wins(self) -> str:
        return "authorized_wins"

    @mutation(policies=[unauthorized_policy])
    async def unauthorized(self) -> str:
        return "unauthorized"

    @mutation(policies=[forbidden_policy])
    async def forbidden(self) -> str:
        return "forbidden"

    @mutation(policies=[unauthorized_policy, forbidden_policy])
    async def forbidden_wins(self) -> str:
        return "mixed"


async def test_security_none():
    assert await R1().none() == "none"


async def test_security_empty():
    assert await R1().empty() == "empty"


async def test_security_authorized():
    assert await R1().authorized() == "authorized"


async def test_security_authorized_wins():
    assert await R1().authorized_wins() == "authorized_wins"


async def test_security_unauthorized():
    with pytest.raises(UnauthorizedError):
        await R1().unauthorized()


async def test_security_forbidden():
    with pytest.raises(ForbiddenError):
        await R1().forbidden()


async def test_security_forbidden_wins():
    with pytest.raises(ForbiddenError):
        await R1().forbidden_wins()
