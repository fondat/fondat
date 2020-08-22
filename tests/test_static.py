import fondat.resource as r
import fondat.schema as s
import pytest

from fondat.static import static_resource
from fondat.resource import operation


schema = s.bytes(format="binary")

content = b"This is the content that will be returned."


@pytest.mark.asyncio
async def test_get():
    resource = static_resource(content, schema)()
    assert await resource.get() == content
