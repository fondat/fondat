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
        with fondat.error.replace_exception(ValueError, RuntimeError):
            raise ValueError


def test_replace_catch_multiple():
    exceptions = (TypeError, ValueError)
    for exception in exceptions:
        with pytest.raises(RuntimeError):
            with fondat.error.replace_exception(exceptions, RuntimeError):
                raise exception


def test_replace_catch_none():
    with pytest.raises(ValueError):
        with fondat.error.replace_exception(TypeError, RuntimeError):
            raise ValueError
