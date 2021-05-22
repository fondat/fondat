import pytest

from fondat.data import copy_data, datacls, make_datacls, derive_datacls
from fondat.types import is_optional
from dataclasses import field, fields
from typing import Optional


def test_datacls_optional():
    @datacls
    class Foo:
        x: Optional[int]

    foo = Foo()
    assert foo.x == None


def test_datacls_default():
    @datacls
    class Foo:
        x: int = 1

    foo = Foo()
    assert foo.x == 1


def test_datacls_field_default():
    @datacls
    class Foo:
        x: int = field(default=1)

    foo = Foo()
    assert foo.x == 1


def test_datacls_field_default_factory():
    @datacls
    class Foo:
        x: dict = field(default_factory=dict)

    foo = Foo()
    assert foo.x == {}


def test_make_datacls_optional():
    Foo = make_datacls("Foo", {"x": Optional[int]}.items())
    foo = Foo()
    assert foo.x == None


def test_make_datacls_field_default():
    Foo = make_datacls("Foo", (("x", int, field(default=1)),))
    foo = Foo()
    assert foo.x == 1


def test_make_datacls_field_default_factory():
    Foo = make_datacls("Foo", (("x", dict, field(default_factory=dict)),))
    foo = Foo()
    assert foo.x == {}


def test_derive_datacls():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = derive_datacls("Bar", Foo)
    assert Bar.__annotations__.keys() == Foo.__annotations__.keys()


def test_derive_datacls_include():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = derive_datacls("Bar", Foo, include={"a", "b"})
    assert Bar.__annotations__.keys() == {"a", "b"}


def test_derive_datacls_exclude():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = derive_datacls("Bar", Foo, exclude={"c"})
    assert Bar.__annotations__.keys() == {"a", "b"}


def test_derive_dataclass_append():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = derive_datacls("Bar", Foo, append=(("d", str),))
    assert Bar.__annotations__.keys() == {"a", "b", "c", "d"}
    assert Bar.__annotations__["d"] is str


def test_derive_datacls_optional_true():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = derive_datacls("Bar", Foo, optional=True)
    for field in fields(Bar):
        assert is_optional(field.type)


def test_subset_dataclass_optional_subset():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    optional = {"a", "b"}
    Bar = derive_datacls("Bar", Foo, optional=optional)
    for f in fields(Bar):
        assert (f.name in optional and is_optional(f.type)) or not is_optional(f.type)


def test_copy_all():
    Foo = make_datacls(
        "Foo", (("a", Optional[int]), ("b", Optional[str]), ("c", Optional[float]))
    )
    foo = Foo(a=1, b="a", c=2.0)
    bar = Foo()
    copy_data(foo, bar)
    assert foo == bar


def test_copy_subset():
    Foo = make_datacls(
        "Foo", (("a", Optional[int]), ("b", Optional[str]), ("c", Optional[float]))
    )
    foo = Foo(a=1, b="a", c=2.0)
    bar = Foo()
    subset = {"a", "b"}
    copy_data(foo, bar, subset)
    for name in bar.__annotations__:
        assert getattr(bar, name) == (getattr(foo, name) if name in subset else None)


def test_copy_common():
    Foo = make_datacls("Foo", (("a", int), ("b", str), ("c", float)))
    Bar = make_datacls("Bar", (("a", Optional[int]), ("b", Optional[str])))
    foo = Foo(a=1, b="a", c=2.0)
    bar = Bar()
    copy_data(foo, bar)
    for name in bar.__annotations__:
        assert getattr(bar, name) == getattr(foo, name)
