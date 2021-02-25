import pytest
import fondat.error

from http import HTTPStatus
from fondat.error import get_error_for_status


def test_get_error_int():
    assert get_error_for_status(400) == fondat.error.BadRequestError
    assert get_error_for_status(404) == fondat.error.NotFoundError
    assert get_error_for_status(200) == fondat.error.InternalServerError
    assert get_error_for_status(200, None) is None


def test_get_error_http_status():
    assert get_error_for_status(HTTPStatus.BAD_REQUEST) == fondat.error.BadRequestError
    assert get_error_for_status(HTTPStatus.NOT_FOUND) == fondat.error.NotFoundError
    assert get_error_for_status(HTTPStatus.OK) == fondat.error.InternalServerError
    assert get_error_for_status(HTTPStatus.OK, None) is None
