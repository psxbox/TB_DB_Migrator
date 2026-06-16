from typing import Any, Tuple


def try_cast_string(value: Any) -> Tuple[str, Any]:
    """Try to cast str_v to long_v or dbl_v. Returns (column_name, value)."""
    if value is None:
        return "str_v", None
    try:
        return "long_v", int(value)
    except (ValueError, TypeError):
        try:
            return "dbl_v", float(value)
        except (ValueError, TypeError):
            return "str_v", value
