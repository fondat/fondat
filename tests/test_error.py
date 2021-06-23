import pytest

import fondat.error

from http import HTTPStatus
from fondat.error import error_for_status


def test_get_error_int():
    assert error_for_status(400) == fondat.error.BadRequestError
    assert error_for_status(404) == fondat.error.NotFoundError
    assert error_for_status(200) == fondat.error.InternalServerError
    assert error_for_status(200, None) is None


def test_get_error_http_status():
    assert error_for_status(HTTPStatus.BAD_REQUEST) == fondat.error.BadRequestError
    assert error_for_status(HTTPStatus.NOT_FOUND) == fondat.error.NotFoundError
    assert error_for_status(HTTPStatus.OK) == fondat.error.InternalServerError
    assert error_for_status(HTTPStatus.OK, None) is None


def test_replace_catch_single():
    with pytest.raises(RuntimeError):
        with fondat.error.replace(ValueError, RuntimeError):
            raise ValueError


def test_replace_catch_multiple():
    exceptions = (TypeError, ValueError)
    for exception in exceptions:
        with pytest.raises(RuntimeError):
            with fondat.error.replace(exceptions, RuntimeError):
                raise exception


def test_replace_catch_none():
    with pytest.raises(ValueError):
        with fondat.error.replace(TypeError, RuntimeError):
            raise ValueError


def test_replace_args():
    try:
        with fondat.error.replace(TypeError, fondat.error.BadRequestError):
            raise TypeError("error_message")
    except fondat.error.BadRequestError as bre:
        assert bre.args[0] == "error_message"


def test_append():
    try:
        with fondat.error.append(Exception, " in spaaace!"):
            raise RuntimeError("Pigs")
    except RuntimeError as re:
        assert re.args[0] == "Pigs in spaaace!"
