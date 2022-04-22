import math
import re
from statistics import median
from textwrap import dedent
from typing import TYPE_CHECKING
import logging


import sqlglot
from sqlglot.expressions import EQ, Column, Identifier

from .default_from_jsonschema import default_value_from_schema
from .input_column import InputColumn
from .misc import dedupe_preserving_order, normalise, interpolate
from .parse_sql import get_columns_used_from_sql


# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .settings import Settings

logger = logging.getLogger(__name__)


def _is_exact_match(sql):
    syntax_tree = sqlglot.parse_one(sql, read="spark")

    expected_types = {0: EQ, 1: Column, 2: Identifier}
    for tup in syntax_tree.walk():
        subtree = tup[0]
        depth = getattr(subtree, "depth", None)
        expected_type = expected_types.get(depth, None)
        if expected_type:
            if not type(subtree) is expected_type:
                return False
    return True


def _exact_match_colname(sql):
    cols = []
    syntax_tree = sqlglot.parse_one(sql.lower(), read="spark")
    for tup in syntax_tree.walk():
        subtree = tup[0]
        depth = getattr(subtree, "depth", None)
        if depth == 2:
            cols.append(subtree.this)

    cols = [c[:-2] for c in cols]  # Remove _l and _r
    cols = list(set(cols))
    if len(cols) != 1:
        raise ValueError(
            f"Expected sql condition to refer to one column but got {cols}"
        )
    return cols[0]


class ComparisonLevel:
    def __init__(self, level_dict, comparison=None, settings_obj: "Settings" = None):

        self._level_dict = (
            level_dict  # Protected, because we don't want to modify the original dict
        )
        self.comparison = comparison
        self.settings_obj = settings_obj

        self.sql_condition = self._level_dict["sql_condition"]
        self.is_null_level = self.level_dict_val_else_default("is_null_level")
        self.tf_adjustment_weight = self.level_dict_val_else_default(
            "tf_adjustment_weight"
        )

        self.tf_minimum_u_value = self.level_dict_val_else_default("tf_minimum_u_value")

        # Private values controlled with getter/setter
        self._m_probability = self._level_dict.get("m_probability")
        self._u_probability = self._level_dict.get("u_probability")

        # Enable the level to 'know' when it's been trained
        self.trained_m_probabilities = []
        self.trained_u_probabilities = []

        self.validate()

    def level_dict_val_else_default(self, key):
        val = self._level_dict.get(key)
        if not val:
            val = default_value_from_schema(key, "comparison_level")
        return val

    @property
    def tf_adjustment_input_column(self):

        val = self.level_dict_val_else_default("tf_adjustment_column")
        if val:
            return InputColumn(val, tf_adjustments=True, settings_obj=self.settings_obj)
        else:
            return None

    @property
    def tf_adjustment_input_column_name(self):
        input_column = self.tf_adjustment_input_column
        if input_column:
            return input_column.input_name
        else:
            return None

    @property
    def m_probability(self):
        if self.is_null_level:
            return None
        if self._m_probability == "level not observed in training dataset":
            return 1e-6
        if self._m_probability is None:
            vals = normalise(interpolate(0.05, 0.95, self.comparison.num_levels))
            return vals[self.comparison_vector_value]
        return self._m_probability

    @m_probability.setter
    def m_probability(self, value):
        if self.is_null_level:
            raise AttributeError("Cannot set m_probability when is_null_level is true")
        if value == "level not observed in training dataset":
            cc_n = self.comparison.comparison_name
            cl_n = self.label_for_charts
            logger.warn(
                "\nWARNING:\n"
                f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                "unable to train m value"
            )

        self._m_probability = value

    @property
    def u_probability(self):
        if self.is_null_level:
            return None
        if self._u_probability == "level not observed in training dataset":
            return 1e-6
        if self._u_probability is None:
            vals = normalise(interpolate(0.95, 0.05, self.comparison.num_levels))
            return vals[self.comparison_vector_value]
        return self._u_probability

    @u_probability.setter
    def u_probability(self, value):
        if self.is_null_level:
            raise AttributeError("Cannot set u_probability when is_null_level is true")
        if value == "level not observed in training dataset":
            cc_n = self.comparison.comparison_name
            cl_n = self.label_for_charts
            logger.warn(
                "\nWARNING:\n"
                f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                "unable to train u value"
            )
        self._u_probability = value

    @property
    def m_probability_description(self):
        if self.m_probability is not None:
            return (
                "Amongst truly matching record comparisons, "
                f"{self.m_probability:.2%} of records are in the "
                f"{self.label_for_charts.lower()} comparison level"
            )

    @property
    def u_probability_description(self):
        if self.u_probability is not None:
            return (
                "Amongst truly non-matching record comparisons, "
                f"{self.u_probability:.2%} of records are in the "
                f"{self.label_for_charts.lower()} comparison level"
            )

    def add_trained_u_probability(self, val, desc="no description given"):

        self.trained_u_probabilities.append(
            {"probability": val, "description": desc, "m_or_u": "u"}
        )

    def add_trained_m_probability(self, val, desc="no description given"):

        self.trained_m_probabilities.append(
            {"probability": val, "description": desc, "m_or_u": "m"}
        )

    @property
    def has_estimated_u_values(self):
        if self.is_null_level:
            return True
        vals = [r["probability"] for r in self.trained_u_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return len(vals) > 0

    @property
    def has_estimated_m_values(self):
        if self.is_null_level:
            return True
        vals = [r["probability"] for r in self.trained_m_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return len(vals) > 0

    @property
    def has_estimated_values(self):
        return self.has_estimated_m_values and self.has_estimated_u_values

    @property
    def trained_m_median(self):
        vals = [r["probability"] for r in self.trained_m_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        if len(vals) == 0:
            return None
        return median(vals)

    @property
    def trained_u_median(self):
        vals = [r["probability"] for r in self.trained_u_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        if len(vals) == 0:
            return None
        return median(vals)

    @property
    def m_is_trained(self):
        if self.is_null_level:
            return True
        if self._m_probability == "level not observed in data":
            return False
        if self._m_probability is None:
            return False
        return True

    @property
    def u_is_trained(self):
        if self.is_null_level:
            return True
        if self._u_probability == "level not observed in data":
            return False
        if self._u_probability is None:
            return False
        return True

    @property
    def is_trained(self):
        return self.m_is_trained and self.u_is_trained

    @property
    def bayes_factor(self):
        if self.is_null_level:
            return 1.0
        if self.m_probability is None or self.u_probability is None:
            return None
        else:
            return self.m_probability / self.u_probability

    @property
    def log2_bayes_factor(self):
        if self.is_null_level:
            return 0.0
        else:
            return math.log2(self.bayes_factor)

    @property
    def bayes_factor_description(self):
        text = (
            f"If comparison level is `{self.label_for_charts.lower()}` "
            "then comparison is"
        )
        if self.bayes_factor >= 1.0:
            return f"{text} {self.bayes_factor:,.2f} times more likely to be a match"
        else:
            mult = 1 / self.bayes_factor
            return f"{text}  {mult:,.2f} times less likely to be a match"

    @property
    def label_for_charts(self):
        return self._level_dict.get(
            "label_for_charts", str(self.comparison_vector_value)
        )

    @property
    def is_else_level(self):
        if self.sql_condition.strip().upper() == "ELSE":
            return True

    @property
    def has_tf_adjustments(self):
        col = self._level_dict.get("tf_adjustment_column")
        return col is not None

    def _validate_sql(self):
        sql = self.sql_condition
        if self.is_else_level:
            return True

        try:
            sqlglot.parse_one(sql)
        except sqlglot.ParseError as e:
            raise ValueError(f"Error parsing sql_statement:\n{sql}") from e

        return True

    @property
    def input_columns_used_by_sql_condition(self):
        # returns e.g. InputColumn(first_name), InputColumn(surname)

        if self.is_else_level:
            return []

        cols = get_columns_used_from_sql(self.sql_condition)
        # Parsed order seems to be roughly in reverse order of apearance
        cols = cols[::-1]

        cols = [re.sub(r"_L$|_R$", "", c, flags=re.IGNORECASE) for c in cols]
        cols = dedupe_preserving_order(cols)

        input_cols = []
        for c in cols:

            # We could have tf adjustments for surname on a dmeta_surname column
            # If so, we want to set the tf adjustments against the surname col,
            # not the dmeta_surname one
            if c == self.tf_adjustment_input_column_name:
                input_cols.append(
                    InputColumn(c, tf_adjustments=True, settings_obj=self.settings_obj)
                )
            else:
                input_cols.append(
                    InputColumn(c, tf_adjustments=False, settings_obj=self.settings_obj)
                )

        return input_cols

    @property
    def columns_to_select_for_blocking(self):
        # e.g. l.first_name as first_name_l, r.first_name as first_name_r
        output_cols = []
        cols = self.input_columns_used_by_sql_condition

        for c in cols:
            output_cols.extend(c.l_r_names_as_l_r)
            output_cols.extend(c.l_r_tf_names_as_l_r)

        return dedupe_preserving_order(output_cols)

    @property
    def when_then_comparison_vector_value_sql(self):
        # e.g. when first_name_l = first_name_r then 1
        if not hasattr(self, "comparison_vector_value"):
            raise ValueError(
                "Cannot get the 'when .. then ...' sql expression because "
                "this comparison level does not have a parent ComparisonColumn. "
                "The comparison_vector_value is only defined in the "
                "context of a list of ComparisonLevels within a ComparisonColumn."
            )
        if self.is_else_level:
            return f"{self.sql_condition} {self.comparison_vector_value}"
        else:
            return f"WHEN {self.sql_condition} THEN {self.comparison_vector_value}"

    @property
    def is_exact_match(self):
        if self.is_else_level:
            return False

        sqls = re.split(r" and ", self.sql_condition, flags=re.IGNORECASE)
        for sql in sqls:
            if not _is_exact_match(sql):
                return False
        return True

    @property
    def exact_match_colnames(self):

        sqls = re.split(r" and ", self.sql_condition, flags=re.IGNORECASE)
        for sql in sqls:
            if not _is_exact_match(sql):
                raise ValueError(
                    "sql_cond not an exact match so can't get exact match column name"
                )

        cols = []
        for sql in sqls:
            col = _exact_match_colname(sql)
            cols.append(col)
        return cols

    @property
    def u_probability_corresponding_to_exact_match(self):
        levels = self.comparison.comparison_levels

        # Find a level with a single exact match colname
        # which is equal to the tf adjustment input colname

        for level in levels:
            if not level.is_exact_match:
                continue
            colnames = level.exact_match_colnames
            if len(colnames) != 1:
                continue
            if colnames[0] == self.tf_adjustment_input_column_name:
                return level.u_probability
        raise ValueError(
            f"Could not find exact match level for {self.tf_adjustment_input_col_name}."
            "\nAn exact match level is required to make a term frequency adjustment "
            "on a comparison level that is not an exact match."
        )

    @property
    def bayes_factor_sql(self):
        sql = f"""
        WHEN
        {self.comparison.gamma_column_name} = {self.comparison_vector_value}
        THEN {self.bayes_factor}D
        """
        return dedent(sql)

    @property
    def tf_adjustment_sql(self):
        gamma_column_name = self.comparison.gamma_column_name
        gamma_colname_value_is_this_level = (
            f"{gamma_column_name} = {self.comparison_vector_value}"
        )

        # A tf adjustment of 1D is a multiplier of 1.0, i.e. no adjustment
        if self.comparison_vector_value == -1:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then 1D"
        elif not self.has_tf_adjustments:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then 1D"
        elif self.tf_adjustment_weight == 0:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then 1D"
        elif self.is_else_level:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then 1D"
        else:
            tf_adj_col = self.tf_adjustment_input_column

            coalesce_l_r = f"coalesce({tf_adj_col.tf_name_l}, {tf_adj_col.tf_name_r})"
            coalesce_r_l = f"coalesce({tf_adj_col.tf_name_r}, {tf_adj_col.tf_name_l})"

            tf_adjustment_exists = f"{coalesce_l_r} is not null"
            u_prob_exact_match = self.u_probability_corresponding_to_exact_match

            # Using coalesce protects against one of the tf adjustments being null
            # Which would happen if the user provided their own tf adjustment table
            # That didn't contain some of the values in this data

            # In this case rather than taking the greater of the two, we take
            # whichever value exists

            if self.tf_minimum_u_value == 0.0:
                divisor_sql = f"""
                (CASE
                    WHEN {coalesce_l_r} >= {coalesce_r_l}
                    THEN {coalesce_l_r}
                    ELSE {coalesce_r_l}
                END)
                """
            else:
                # This sql works correctly even when the tf_minimum_u_value is 0.0
                # but is less efficient to execute, hence the above if statement
                divisor_sql = f"""
                (CASE
                    WHEN {coalesce_l_r} >= {coalesce_r_l}
                    AND {coalesce_l_r} > {self.tf_minimum_u_value}D
                        THEN {coalesce_l_r}
                    WHEN {coalesce_r_l}  > {self.tf_minimum_u_value}D
                        THEN {coalesce_r_l}
                    ELSE {self.tf_minimum_u_value}D
                END)
                """

            sql = f"""
            WHEN  {gamma_colname_value_is_this_level} then
                (CASE WHEN {tf_adjustment_exists}
                THEN
                POW(
                    {u_prob_exact_match}D /{divisor_sql},
                    {self.tf_adjustment_weight}D
                )
                ELSE 1D
                END)
            """
        return dedent(sql).strip()

    @property
    def as_dict(self):
        "The minimal representation of this level to use as an input to Splink"
        output = {}

        output["sql_condition"] = self.sql_condition

        if self._level_dict.get("label_for_charts"):
            output["label_for_charts"] = self.label_for_charts

        if self.m_probability:
            output["m_probability"] = self.m_probability

        if self.u_probability:
            output["u_probability"] = self.u_probability

        if self.has_tf_adjustments:
            output["tf_adjustment_column"] = self.tf_adjustment_input_column.input_name
            if self.tf_adjustment_weight != 0:
                output["tf_adjustment_weight"] = self.tf_adjustment_weight

        if self.is_null_level:
            output["is_null_level"] = True
        return output

    @property
    def as_completed_dict(self):
        comp_dict = self.as_dict
        comp_dict["comparison_vector_value"] = self.comparison_vector_value
        return comp_dict

    @property
    def as_detailed_record(self):
        "A detailed representation of this level to describe it in charting outputs"
        output = {}
        output["sql_condition"] = self.sql_condition
        output["label_for_charts"] = self.label_for_charts

        output["m_probability"] = self.m_probability
        output["u_probability"] = self.u_probability

        output["m_probability_description"] = self.m_probability_description
        output["u_probability_description"] = self.u_probability_description

        output["has_tf_adjustments"] = self.has_tf_adjustments
        if self.has_tf_adjustments:
            output["tf_adjustment_column"] = self.tf_adjustment_input_column.input_name
        else:
            output["tf_adjustment_column"] = None
        output["tf_adjustment_weight"] = self.tf_adjustment_weight

        output["is_null_level"] = self.is_null_level
        output["bayes_factor"] = self.bayes_factor
        output["log2_bayes_factor"] = self.log2_bayes_factor
        output["comparison_vector_value"] = self.comparison_vector_value
        output["max_comparison_vector_value"] = self.comparison.num_levels - 1
        output["bayes_factor_description"] = self.bayes_factor_description

        return output

    @property
    def parameter_estimates_as_records(self):

        output_records = []

        cl_record = self.as_detailed_record
        trained_values = self.trained_u_probabilities + self.trained_m_probabilities
        for trained_value in trained_values:
            record = {}
            record["m_or_u"] = trained_value["m_or_u"]
            p = trained_value["probability"]
            record["estimated_probability"] = p
            record["estimate_description"] = trained_value["description"]
            if p is not None and p > 0.0:
                record["estimated_probability_as_log_odds"] = math.log2(p / (1 - p))
            else:
                record["estimated_probability_as_log_odds"] = None

            record["sql_condition"] = cl_record["sql_condition"]
            record["comparison_level_label"] = cl_record["label_for_charts"]
            record["comparison_vector_value"] = cl_record["comparison_vector_value"]
            output_records.append(record)

        return output_records

    def validate(self):
        self._validate_sql()

    def __repr__(self):
        sql = self.sql_condition
        sql = (sql[:75] + "...") if len(sql) > 75 else sql
        return (
            f"<ComparisonLevel {self.label_for_charts} with "
            f"match weight {self.bayes_factor:,.2} and SQL: {sql}>"
        )
