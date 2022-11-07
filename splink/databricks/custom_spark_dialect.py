from sqlglot import exp

from sqlglot.dialects import Dialect, Spark
from sqlglot.generator import Generator


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
        if expression.this.key == "literal":
            return f"{expression.name}D"

    return expression.sql(dialect="spark")


class CustomSpark(Spark):
    class Parser(Spark.Parser):
        FUNCTIONS = {
            **Spark.Parser.FUNCTIONS,
        }

    class Generator(Generator):

        TYPE_MAPPING = {
            **Spark.Generator.TYPE_MAPPING,
        }

        TRANSFORMS = {
            **Spark.Generator.TRANSFORMS,
            exp.Cast: cast_as_double_edit,
            exp.TryCast: cast_as_double_edit,
        }


Dialect["customspark"]
