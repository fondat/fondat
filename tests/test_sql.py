from fondat.sql import Expression


def test_expression_params():
    expr = Expression("1", "2")
    assert str(expr) == "12"


def test_expression_join():
    exprs = []
    for n in range(2):
        exprs.append(Expression.join((Expression(str(n)) for n in range(3)), "."))
    assert str(Expression.join(exprs, ":")) == "0.1.2:0.1.2"
