import pytest

from fondat.annotation import Password
from fondat.data import (
    copy_data,
    datacls,
    make_datacls,
    derive_datacls,
    derive_typeddict,
    redact_passwords,
)
from fondat.types import is_optional
from dataclasses import asdict, field, fields
from typing import Annotated, Optional, TypedDict


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
    bar = copy_data(foo, Foo)
    assert foo == bar


def test_copy_include():
    @datacls
    class Foo:
        a: Optional[int]
        b: Optional[str]
        c: Optional[float]

    foo = Foo(a=1, b="a", c=2.0)
    include = {"a", "b"}
    bar = copy_data(foo, Foo, include=include)
    for name in bar.__annotations__:
        assert getattr(bar, name) == (getattr(foo, name) if name in include else None)


def test_copy_exclude():
    @datacls
    class Foo:
        a: Optional[int]
        b: Optional[str]
        c: Optional[float]

    foo = Foo(a=1, b="a", c=2.0)
    exclude = {"a"}
    bar = copy_data(foo, Foo, exclude=exclude)
    for name in bar.__annotations__:
        assert getattr(bar, name) == (getattr(foo, name) if name not in exclude else None)


def test_copy_td_to_dc():
    fields = (("a", str), ("b", int))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    td = TD(a="a", b=1)
    assert copy_data(td, DC) == DC(**td)


def test_copy_td_to_dc_include():
    fields = (("a", Optional[str]), ("b", Optional[int]))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    td = TD(a="a", b=1)
    assert copy_data(td, DC, include={"a"}) == DC(a="a")


def test_copy_td_to_dc_exclude():
    fields = (("a", Optional[str]), ("b", Optional[int]))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    td = TD(a="a", b=1)
    assert copy_data(td, DC, exclude={"a"}) == DC(b=1)


def test_copy_dc_to_td():
    fields = (("a", str), ("b", int))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    dc = DC(a="a", b=1)
    assert copy_data(dc, TD) == asdict(dc)


def test_copy_dc_to_td_include():
    fields = (("a", Optional[str]), ("b", Optional[int]))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    dc = DC(a="a", b=1)
    assert copy_data(dc, TD, include={"a"}) == {"a": "a"}


def test_copy_dc_to_td_exclude():
    fields = (("a", Optional[str]), ("b", Optional[int]))
    DC = make_datacls("DC", fields)
    TD = TypedDict("TD", fields)
    dc = DC(a="a", b=1)
    assert copy_data(dc, TD, exclude={"a"}) == {"b": 1}


def test_derive_typeddict_dataclass_simple():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = derive_typeddict("TD", DC)
    annotations = TD.__annotations__
    assert annotations["a"] is str
    assert annotations["b"] is int
    assert annotations["c"] is float


def test_derive_typeddict_dataclass_include():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = derive_typeddict("TD", DC, include={"a", "b"})
    assert TD.__annotations__.keys() == {"a", "b"}


def test_derive_typeddict_dataclass_exclude():
    DC = make_datacls("A", (("a", str), ("b", int), ("c", float)))
    TD = derive_typeddict("TD", DC, exclude={"a"})
    assert TD.__annotations__.keys() == {"b", "c"}


def test_derive_typeddict_annotated_dataclass():
    DC = make_datacls("A", (("a", str),))
    TD = derive_typeddict("TD", Annotated[DC, "annotated"])
    assert TD.__annotations__.keys() == DC.__annotations__.keys()


def test_redaction():
    @datacls
    class DC:
        username: str
        password: Annotated[str, Password]

    dc = DC(username="username", password="passsword")
    redact_passwords(DC, dc)
    dc == DC(username="username", password="__REDACTED__")
