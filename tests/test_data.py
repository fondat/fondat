import pytest

from fondat.data import datacls, make_datacls
from dataclasses import field
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
