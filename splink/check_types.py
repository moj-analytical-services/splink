"""
**Functions**
.. autofunction:: block_using_rules
"""

from typing import get_type_hints, Union

try:
    from pyspark.sql.dataframe import DataFrame
    from pyspark.sql.session import SparkSession
    spark_exists = True
except ImportError:
    DataFrame = None
    SparkSession = None
    spark_exists = False

from functools import wraps


def check_types(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        type_hints = get_type_hints(func)

        args_names = func.__code__.co_varnames[:func.__code__.co_argcount]
        args_dict = {**dict(zip(args_names, args)), **kwargs}


        for key in type_hints:
            if key in args_dict:
                this_arg = args_dict[key]
                this_type_hint = type_hints[key]

                # If it's of type union it will have the __args__ argument
                try:
                    if this_type_hint.__origin__ == Union:
                        possible_types = this_type_hint.__args__
                    else:
                        possible_types = (this_type_hint,)
                except AttributeError:
                    possible_types = (this_type_hint,)

                if isinstance(this_arg, possible_types):
                    pass
                else:
                    poss_types_str = [str(t) for t in possible_types]
                    poss_types_str = ' or '.join(poss_types_str)
                    raise TypeError(f"You passed the wrong type for argument {key}. "
                                    f"You passed the argument {args_dict[key]} of type {type(args_dict[key])}. "
                                    f"The type for this argument should be {poss_types_str}. ")


        return func(*args, **kwargs)

    return wrapper