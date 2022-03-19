import sqlglot
import sqlglot.expressions as exp

from sqlglot.dialects import Dialect
import sqlglot.expressions as exp
from sqlglot.dialects import _no_tablesample_sql
from sqlglot.trie import new_trie

def _refactor_levenshtein(node):
    if isinstance(node, exp.Anonymous) and node.text("this").upper() == "LEVENSHTEIN":
        # also errors if nothing exists to the right (as it should...)
        if not isinstance(node.parent.right, exp.Literal) or not node.parent.right:
            raise ValueError(f"LEVENSHTEIN function has nothing to evalute against")
        node.args["this"] = "dbo.LEVENSHTEIN"
        # check literal isn't already in place
        keys = [not expr.is_int if expr.key == 'literal' else False for expr in node.args["expressions"]]
        if not any(keys):
            node.args["expressions"].append(sqlglot.parse_one("2"))
    return node


class MSSQL(Dialect):

    """
    To do:
    * limit
    * ifnull
    * create table -- partially done
    """

    def _tsql_bool(self, expression):
        bool_val = expression.this
        return "1=1" if bool_val else "0=1"

    def _refactor_levenshtein(self, expression):
        if expression.text("this").upper() == "LEVENSHTEIN":
            # also error if we have no value to use as our max (should be explicitly specified by the user)
            if not isinstance(expression.parent.right, exp.Literal) or not expression.parent.right:
                raise ValueError(f"LEVENSHTEIN function has nothing to evalute against")
            expression.args["this"] = "dbo.LEVENSHTEIN"
            # check literal isn't already in place
            keys = [not expr.is_int if expr.key == 'literal' else False for expr in expression.args["expressions"]]
            if not any(keys):
                expression.args["expressions"].append(sqlglot.parse_one("2"))
        return expression.sql()

    type_mapping = {
        exp.DataType.Type.BOOLEAN: "INTEGER",
        exp.DataType.Type.TINYINT: "INTEGER",
        exp.DataType.Type.SMALLINT: "INTEGER",
        exp.DataType.Type.INT: "INTEGER",
        exp.DataType.Type.BIGINT: "INTEGER",
        exp.DataType.Type.FLOAT: "REAL",
        exp.DataType.Type.DOUBLE: "REAL",
        exp.DataType.Type.DECIMAL: "REAL",
        exp.DataType.Type.CHAR: "TEXT",
        exp.DataType.Type.VARCHAR: "TEXT",
        exp.DataType.Type.BINARY: "BLOB",
    }

    transforms = {
        exp.TableSample: _no_tablesample_sql,
        exp.Pow: lambda self, e: f"POWER({self.sql(e, 'this')}, {self.sql(e, 'power')})",
        exp.Log2: lambda self, e: f"LOG({self.sql(e, 'this')}, 2)",
        exp.Log10: lambda self, e: f"LOG({self.sql(e, 'this')}, 2)",
        exp.Boolean: _tsql_bool,
        exp.Anonymous: _refactor_levenshtein,
    }


for d in Dialect.classes.values():
    d.time_trie = new_trie(d.time_mapping)
    d.inverse_time_mapping = {v: k for k, v in d.time_mapping.items()}
    d.inverse_time_trie = new_trie(d.inverse_time_mapping)
