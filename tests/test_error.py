import fondat.error as error


def test_get_error_code():
    assert error.errors[400] == error.BadRequestError
    assert error.errors[404] == error.NotFoundError
    assert error.errors[500] == error.InternalServerError


def test_wrap_exception():
    try:
        with error.wrap_exception(catch=ValueError, throw=RuntimeError):
            raise ValueError("oops")
    except RuntimeError as re:
        cause = re.__cause__
        assert type(cause) is ValueError
        assert cause.args == ("oops",)
