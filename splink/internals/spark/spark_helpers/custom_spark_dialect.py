from sqlglot import exp
from sqlglot.dialects import Dialect, Spark


def cast_as_double_edit(self, expression):
    """
    Forces floating point numbers to the double class in spark,
    instead of casting them to decimals.
    This resolves the spark issue where decimals only support precision
    up to 38 digits.
    More info: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    """
    datatype = self.sql(expression, "to")
    if datatype.lower() == "double":
        if expression.this.key == "literal" and expression.name != "Infinity":
            return f"{expression.name}D"

    return expression.sql(dialect="spark")


# later sqlglot version lacks some typing info, so get errors
# e.g. Class cannot subclass "Spark" (has type "Any")  [misc]
class CustomSpark(Spark):  # type: ignore[misc]
    class Parser(Spark.Parser):  # type: ignore[misc]
        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
        }

    class Generator(Spark.Generator):  # type: ignore[misc]
        TYPE_MAPPING = {
            **Spark.Generator.TYPE_MAPPING,
        }

        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.Cast: cast_as_double_edit,
            exp.TryCast: cast_as_double_edit,
        }


Dialect["customspark"]
