from .comparison_level_creator import ComparisonLevelCreator


class NullLevel(ComparisonLevelCreator):

    def create_sql(self, sql_dialect):
        col = self.input_column(sql_dialect)
        return f"{col.name_l()} IS NULL OR {col.name_r()} IS NULL"

    def create_label_for_charts(self):
        return f"{self.col_name} is NULL"

class ElseLevel(ComparisonLevelCreator):
    def create_sql(self, sql_dialect):
        return "ELSE"

    def create_label_for_charts(self):
        return "All other comparisons"


class ExactMatchLevel(ComparisonLevelCreator):
    def create_sql(self, sql_dialect):
        col = self.input_column(sql_dialect)
        return f"{col.name_l()} = {col.name_r()}"

    def create_label_for_charts(self):
        return f"Exact match {self.col_name}"

