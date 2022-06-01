import fondat.error as error


def test_get_error_code():
    assert error.errors[400] == error.BadRequestError
    assert error.errors[404] == error.NotFoundError
    assert error.errors[500] == error.InternalServerError
