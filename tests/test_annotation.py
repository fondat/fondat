import pytest

from fondat.annotation import Description, Example, Format


def test_annotation_in():
    annotations = [Format("password")]
    assert Format("password") in annotations


def test_annotation_eq():
    assert Format("password") == Format("password")
    assert Format("password") != Description("password")


def test_annotation_hash():
    assert hash(Format("password")) == hash(Format("password"))
    assert hash(Format("password")) != hash(Description("password"))
    hash(Example([1, 2, 3]))  # should produce a hash without raising exception
