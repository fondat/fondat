import pytest

import fondat.error as error

from http import HTTPStatus


def test_get_error_code():
    assert error.errors[400] == error.BadRequestError
    assert error.errors[404] == error.NotFoundError
    assert error.errors[200] == error.InternalServerError
    assert error.errors[999] == error.InternalServerError


def test_replace_catch_single():
    with pytest.raises(RuntimeError):
        with error.replace(ValueError, RuntimeError):
            raise ValueError


def test_replace_catch_multiple():
    exceptions = (TypeError, ValueError)
    for exception in exceptions:
        with pytest.raises(RuntimeError):
            with error.replace(exceptions, RuntimeError):
                raise exception


def test_replace_catch_none():
    with pytest.raises(ValueError):
        with error.replace(TypeError, RuntimeError):
            raise ValueError


def test_replace_args():
    try:
        with error.replace(TypeError, error.BadRequestError):
            raise TypeError("error_message")
    except error.BadRequestError as bre:
        assert bre.args[0] == "error_message"


def test_append():
    try:
        with error.append(Exception, " in spaaace!"):
            raise RuntimeError("Pigs")
    except RuntimeError as re:
        assert re.args[0] == "Pigs in spaaace!"
