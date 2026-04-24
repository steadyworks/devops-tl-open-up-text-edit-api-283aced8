from typing import Optional, TypeVar

T = TypeVar("T")


def none_throws(value: Optional[T], message: str = "Value cannot be None") -> T:
    if value is None:
        raise Exception(message)
    return value
