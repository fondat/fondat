import pytest

from fondat.data import copy_data, datacls, make_datacls, derive_datacls, dataclass_typeddict
from fondat.types import is_optional
from dataclasses import field, fields
from typing import Annotated, Optional


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


def test_copy_mapped():
    A = make_datacls("A", (("a", str), ("b", str), ("c", str)))
    B = make_datacls("B", (("d", Optional[str]), ("e", Optional[str]), ("f", Optional[str])))
    a = A(a="a", b="b", c="c")
    b = B()
    copy_data(a, b, {"a": "d", "b": "e", "c": "f"})
    assert b == B(d="a", e="b", f="c")


def test_dataclass_typeddict_simple():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = dataclass_typeddict("TD", DC)
    annotations = TD.__annotations__
    assert annotations["a"] is str
    assert annotations["b"] is int
    assert annotations["c"] is float


def test_dataclass_typeddict_include():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = dataclass_typeddict("TD", DC, include={"a", "b"})
    assert TD.__annotations__.keys() == {"a", "b"}


def test_dataclass_typeddict_exclude():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = dataclass_typeddict("TD", DC, exclude={"a"})
    assert TD.__annotations__.keys() == {"b", "c"}


def test_annotated_dataclass_typeddict():
    DC = make_datacls("A", (("a", str),))
    A = Annotated[DC, "annotated"]
    TD = dataclass_typeddict("TD", A)
    assert TD.__annotations__.keys() == DC.__annotations__.keys()
