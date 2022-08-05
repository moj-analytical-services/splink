from sqlglot import exp
from sqlglot.generator import Generator
from sqlglot.dialects import Dialect


def cast_as_double_edit(self, expression):
    datatype = self.sql(expression, "to")
    if datatype.lower() == "double":
        if expression.this.key == "literal":
            return f"{expression.name}D"

    return expression.sql()


class CustomSpark(Dialect):

    class Generator(Generator):
        TRANSFORMS = {
            exp.Cast: cast_as_double_edit,
            }
