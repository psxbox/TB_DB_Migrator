from migrator.cast import try_cast_string


def test_cast_integer_string():
    col, val = try_cast_string("42")
    assert col == "long_v"
    assert val == 42


def test_cast_negative_integer():
    col, val = try_cast_string("-100")
    assert col == "long_v"
    assert val == -100


def test_cast_float_string():
    col, val = try_cast_string("3.14")
    assert col == "dbl_v"
    assert abs(val - 3.14) < 1e-9


def test_cast_plain_string():
    col, val = try_cast_string("hello")
    assert col == "str_v"
    assert val == "hello"


def test_cast_empty_string():
    col, val = try_cast_string("")
    assert col == "str_v"
    assert val == ""


def test_cast_none_returns_str_v():
    col, val = try_cast_string(None)
    assert col == "str_v"
    assert val is None
