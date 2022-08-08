from sqlglot import exp
from sqlglot.generator import Generator
from sqlglot.dialects import Dialect, Spark


def cast_as_double_edit(self, expression):
    datatype = self.sql(expression, "to")
    if datatype.lower() == "double":
        if expression.this.key == "literal":
            return f"{expression.name}D"

    return expression.sql(dialect="spark")


class CustomSpark(Spark):
    class Generator(Generator):
        TRANSFORMS = {
            exp.Cast: cast_as_double_edit,
            exp.TryCast: cast_as_double_edit,
        }


Dialect["customspark"]
