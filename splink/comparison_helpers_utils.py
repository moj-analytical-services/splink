import re
from functools import wraps
from typing import Any, List

from .dialects import SplinkDialect


def distance_match(distance, threshold):
    if distance <= threshold:
        return True
    else:
        return False


def similarity_match(similarity, threshold):
    if similarity >= threshold:
        return True
    else:
        return False


def threshold_match(comparator, score, distance_threshold, similarity_threshold):
    if re.search("distance", comparator):
        return distance_match(score, distance_threshold)
    elif re.search("similarity", comparator):
        return similarity_match(score, similarity_threshold)


def unsupported_splink_dialects(unsupported_dialects: List[str]):
    def decorator(func):
        def wrapper(self, splink_dialect: SplinkDialect, *args, **kwargs):
            if splink_dialect.name in unsupported_dialects:
                raise ValueError(
                    f"Dialect '{splink_dialect.name}' is not supported "
                    f"for {self.__class__.__name__}"
                )
            return func(self, splink_dialect, *args, **kwargs)

        return wrapper

    return decorator


def unsupported_splink_dialect_expression(unsupported_dialects: List[str]):
    def decorator(func):
        @wraps(func)
        def wrapper(self, expression_argument: Any, *args, **kwargs):
            method_name = func.__name__

            if self._sql_dialect is None:
                raise ValueError(
                    f"'{method_name}' requires a valid dialect be "
                    f"passed to the {self.__class__.__name__} class.\n"
                    "Add a valid dialect using `.register_dialect(<dialect>)` "
                    "before continuing."
                )

            if self._sql_dialect.name in unsupported_dialects and expression_argument:
                raise ValueError(
                    f"Dialect '{self._sql_dialect.name}' does not support "
                    f"{method_name}"
                )
            return func(self, expression_argument, *args, **kwargs)

        return wrapper

    return decorator
