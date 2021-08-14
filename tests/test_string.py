import pytest

from fondat.string import Template


pytestmark = pytest.mark.asyncio


async def test_template_start():
    async def resolver(variable: str) -> str:
        return "World" if variable == "foo" else None

    template = Template("${foo}, hello!")
    result = "World, hello!"
    assert await template.resolve(resolver) == result


async def test_template_middle():
    async def resolver(variable: str) -> str:
        return "world" if variable == "foo" else None

    template = Template("Hello, ${foo} and moon!")
    result = "Hello, world and moon!"
    assert await template.resolve(resolver) == result


async def test_template_end():
    async def resolver(variable: str) -> str:
        return "world." if variable == "foo" else None

    template = Template("Hello, ${foo}")
    result = "Hello, world."
    assert await template.resolve(resolver) == result


async def test_template_fail():
    async def resolver(_: str) -> str:
        return None

    template = Template("hello ${foo}!")
    with pytest.raises(ValueError):
        await template.resolve(resolver)


async def test_template_recursive():
    async def resolver(variable: str) -> str:
        return {"1": "${2}", "2": "3"}.get(variable)

    template = Template("${1}")
    result = "3"
    assert await template.resolve(resolver) == result


async def test_template_multi_line_simple():
    async def resolver(variable: str) -> str:
        return {"foo": "bar", "baz": "qux"}.get(variable)

    template = Template("hello ${foo}\ngoodbye ${baz}\n")
    result = "hello bar\ngoodbye qux\n"
    assert await template.resolve(resolver) == result


async def test_template_multi_line_span_ignore():
    async def resolver(variable: str) -> str:
        return {"foo": "bar", "baz": "qux"}.get(variable)

    template = Template("hello ${foo\n}goodbye ${baz}\n")
    result = "hello ${foo\n}goodbye qux\n"
    assert await template.resolve(resolver) == result
