import math
import re
from statistics import median
from textwrap import dedent
from typing import TYPE_CHECKING, List
import logging


import sqlglot
from sqlglot.expressions import EQ, Column, Identifier

from .default_from_jsonschema import default_value_from_schema
from .input_column import InputColumn
from .misc import (
    dedupe_preserving_order,
    interpolate,
    join_list_with_commas_final_and,
    match_weight_to_bayes_factor,
)
from .parse_sql import get_columns_used_from_sql


# https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .comparison import Comparison

logger = logging.getLogger(__name__)


def _is_exact_match(sql):
    syntax_tree = sqlglot.parse_one(sql, read=None)

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
    syntax_tree = sqlglot.parse_one(sql.lower(), read=None)
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


def _default_m_values(num_levels):
    proportion_exact_match = 0.95
    remainder = 1 - proportion_exact_match
    split_remainder = remainder / (num_levels - 1)
    return [split_remainder] * (num_levels - 1) + [proportion_exact_match]


def _default_u_values(num_levels):
    m_vals = _default_m_values(num_levels)
    if num_levels == 2:
        match_weights = [-5]
    else:
        match_weights = interpolate(-5, 3, num_levels - 1)
    match_weights = match_weights + [10]

    u_vals = []
    for m, w in zip(m_vals, match_weights):
        p = match_weight_to_bayes_factor(w)
        u = m / p
        u_vals.append(u)

    return u_vals


class ComparisonLevel:
    """Each ComparisonLevel defines a gradation (category) of similarity within a
    `Comparison`.

    For example, a `Comparison` that uses the first_name and surname columns may
    define three `ComparisonLevel`s:
        An exact match on first name and surname
        First name and surname have a JaroWinkler score of above 0.95
        All other comparisons

    The method used to assess similarity will depend on the type of data - for
    instance, the method used to assess similarity of a company's turnover would be
    different to the method used to assess the similarity of a person's first name.

    To summarise:

    ```
    Data Linking Model
    ├─-- Comparison: Name
    │    ├─-- ComparisonLevel: Exact match on first_name and surname
    │    ├─-- ComparisonLevel: first_name and surname have JaroWinkler > 0.95
    │    ├─-- ComparisonLevel: All other
    ├─-- Comparison: Date of birth
    │    ├─-- ComparisonLevel: Exact match
    │    ├─-- ComparisonLevel: One character difference
    │    ├─-- ComparisonLevel: All other
    ├─-- etc.
    ```
    """

    def __init__(
        self,
        level_dict,
        comparison: "Comparison" = None,
        sql_dialect: str = None,
    ):

        # Protected, because we don't want to modify the original dict
        self._level_dict = level_dict

        self.comparison: "Comparison" = comparison
        self._sql_dialect = sql_dialect

        self._sql_condition = self._level_dict["sql_condition"]
        self._is_null_level = self._level_dict_val_else_default("is_null_level")
        self._tf_adjustment_weight = self._level_dict_val_else_default(
            "tf_adjustment_weight"
        )

        self._tf_minimum_u_value = self._level_dict_val_else_default(
            "tf_minimum_u_value"
        )

        # Private values controlled with getter/setter
        self._m_probability = self._level_dict.get("m_probability")
        self._u_probability = self._level_dict.get("u_probability")

        # These will be set when the ComparisonLevel is passed into a Comparison
        self._comparison_vector_value: int = None
        self._max_level: bool = None

        # Enable the level to 'know' when it's been trained
        self._trained_m_probabilities: list = []
        self._trained_u_probabilities: list = []

        self._validate()

    def _level_dict_val_else_default(self, key):
        val = self._level_dict.get(key)
        if not val:
            val = default_value_from_schema(key, "comparison_level")
        return val

    @property
    def _tf_adjustment_input_column(self):

        val = self._level_dict_val_else_default("tf_adjustment_column")
        if val:
            return InputColumn(val, tf_adjustments=True, sql_dialect=self._sql_dialect)
        else:
            return None

    @property
    def _tf_adjustment_input_column_name(self):
        input_column = self._tf_adjustment_input_column
        if input_column:
            return input_column.input_name
        else:
            return None

    @property
    def _has_comparison(self):
        from .comparison import Comparison

        return isinstance(self.comparison, Comparison)

    @property
    def m_probability(self):
        if self._is_null_level:
            return None
        if self._m_probability == "level not observed in training dataset":
            return 1e-6
        if self._m_probability is None and self._has_comparison:
            vals = _default_m_values(self.comparison._num_levels)
            return vals[self._comparison_vector_value]
        return self._m_probability

    @m_probability.setter
    def m_probability(self, value):
        if self._is_null_level:
            raise AttributeError("Cannot set m_probability when is_null_level is true")
        if value == "level not observed in training dataset":
            cc_n = self.comparison._output_column_name
            cl_n = self._label_for_charts
            logger.warning(
                "\nWARNING:\n"
                f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                "unable to train m value"
            )

        self._m_probability = value

    @property
    def u_probability(self):
        if self._is_null_level:
            return None
        if self._u_probability == "level not observed in training dataset":
            return 1e-6
        if self._u_probability is None:
            vals = _default_u_values(self.comparison._num_levels)
            return vals[self._comparison_vector_value]
        return self._u_probability

    @u_probability.setter
    def u_probability(self, value):
        if self._is_null_level:
            raise AttributeError("Cannot set u_probability when is_null_level is true")
        if value == "level not observed in training dataset":
            cc_n = self.comparison._output_column_name
            cl_n = self._label_for_charts
            logger.warning(
                "\nWARNING:\n"
                f"Level {cl_n} on comparison {cc_n} not observed in dataset, "
                "unable to train u value"
            )
        self._u_probability = value

    @property
    def _m_probability_description(self):
        if self.m_probability is not None:
            return (
                "Amongst matching record comparisons, "
                f"{self.m_probability:.2%} of records are in the "
                f"{self._label_for_charts.lower()} comparison level"
            )

    @property
    def _u_probability_description(self):
        if self.u_probability is not None:
            return (
                "Amongst non-matching record comparisons, "
                f"{self.u_probability:.2%} of records are in the "
                f"{self._label_for_charts.lower()} comparison level"
            )

    def _add_trained_u_probability(self, val, desc="no description given"):

        self._trained_u_probabilities.append(
            {"probability": val, "description": desc, "m_or_u": "u"}
        )

    def _add_trained_m_probability(self, val, desc="no description given"):

        self._trained_m_probabilities.append(
            {"probability": val, "description": desc, "m_or_u": "m"}
        )

    @property
    def _has_estimated_u_values(self):
        if self._is_null_level:
            return True
        vals = [r["probability"] for r in self._trained_u_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return len(vals) > 0

    @property
    def _has_estimated_m_values(self):
        if self._is_null_level:
            return True
        vals = [r["probability"] for r in self._trained_m_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        return len(vals) > 0

    @property
    def _has_estimated_values(self):
        return self._has_estimated_m_values and self._has_estimated_u_values

    @property
    def _trained_m_median(self):
        vals = [r["probability"] for r in self._trained_m_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        if len(vals) == 0:
            return None
        return median(vals)

    @property
    def _trained_u_median(self):
        vals = [r["probability"] for r in self._trained_u_probabilities]
        vals = [v for v in vals if isinstance(v, (int, float))]
        if len(vals) == 0:
            return None
        return median(vals)

    @property
    def _m_is_trained(self):
        if self._is_null_level:
            return True
        if self._m_probability == "level not observed in data":
            return False
        if self._m_probability is None:
            return False
        return True

    @property
    def _u_is_trained(self):
        if self._is_null_level:
            return True
        if self._u_probability == "level not observed in data":
            return False
        if self._u_probability is None:
            return False
        return True

    @property
    def _is_trained(self):
        return self._m_is_trained and self._u_is_trained

    @property
    def _bayes_factor(self):
        if self._is_null_level:
            return 1.0
        if self.m_probability is None or self.u_probability is None:
            return None
        else:
            return self.m_probability / self.u_probability

    @property
    def _log2_bayes_factor(self):
        if self._is_null_level:
            return 0.0
        else:
            return math.log2(self._bayes_factor)

    @property
    def _bayes_factor_description(self):
        text = (
            f"If comparison level is `{self._label_for_charts.lower()}` "
            "then comparison is"
        )
        if self._bayes_factor >= 1.0:
            return f"{text} {self._bayes_factor:,.2f} times more likely to be a match"
        else:
            mult = 1 / self._bayes_factor
            return f"{text}  {mult:,.2f} times less likely to be a match"

    @property
    def _label_for_charts(self):
        return self._level_dict.get(
            "label_for_charts", str(self._comparison_vector_value)
        )

    @property
    def _is_else_level(self):
        if self._sql_condition.strip().upper() == "ELSE":
            return True

    @property
    def _has_tf_adjustments(self):
        col = self._level_dict.get("tf_adjustment_column")
        return col is not None

    @property
    def _sql_read_dialect(self):
        read_dialect = None
        if self._sql_dialect is not None:
            read_dialect = self._sql_dialect
        return read_dialect

    def _validate_sql(self):
        sql = self._sql_condition
        if self._is_else_level:
            return True

        try:
            sqlglot.parse_one(sql, read=self._sql_read_dialect)
        except sqlglot.ParseError as e:
            raise ValueError(f"Error parsing sql_statement:\n{sql}") from e

        return True

    @property
    def _input_columns_used_by_sql_condition(self) -> List[InputColumn]:
        # returns e.g. InputColumn(first_name), InputColumn(surname)

        if self._is_else_level:
            return []

        cols = get_columns_used_from_sql(
            self._sql_condition, dialect=self._sql_read_dialect
        )
        # Parsed order seems to be roughly in reverse order of apearance
        cols = cols[::-1]

        cols = [re.sub(r"_L$|_R$", "", c, flags=re.IGNORECASE) for c in cols]
        cols = dedupe_preserving_order(cols)

        input_cols = []
        for c in cols:

            # We could have tf adjustments for surname on a dmeta_surname column
            # If so, we want to set the tf adjustments against the surname col,
            # not the dmeta_surname one
            if c == self._tf_adjustment_input_column_name:
                input_cols.append(
                    InputColumn(c, tf_adjustments=True, sql_dialect=self._sql_dialect)
                )
            else:
                input_cols.append(
                    InputColumn(c, tf_adjustments=False, sql_dialect=self._sql_dialect)
                )

        return input_cols

    @property
    def _columns_to_select_for_blocking(self):
        # e.g. l.first_name as first_name_l, r.first_name as first_name_r
        output_cols = []
        cols = self._input_columns_used_by_sql_condition

        for c in cols:
            output_cols.extend(c.l_r_names_as_l_r())
            output_cols.extend(c.l_r_tf_names_as_l_r())

        return dedupe_preserving_order(output_cols)

    @property
    def _when_then_comparison_vector_value_sql(self):
        # e.g. when first_name_l = first_name_r then 1
        if not hasattr(self, "_comparison_vector_value"):
            raise ValueError(
                "Cannot get the 'when .. then ...' sql expression because "
                "this comparison level does not belong to a parent Comparison. "
                "The comparison_vector_value is only defined in the "
                "context of a list of ComparisonLevels within a Comparison."
            )
        if self._is_else_level:
            return f"{self._sql_condition} {self._comparison_vector_value}"
        else:
            return f"WHEN {self._sql_condition} THEN {self._comparison_vector_value}"

    @property
    def _is_exact_match(self):
        if self._is_else_level:
            return False

        sqls = re.split(r" and ", self._sql_condition, flags=re.IGNORECASE)
        for sql in sqls:
            if not _is_exact_match(sql):
                return False
        return True

    @property
    def _exact_match_colnames(self):

        sqls = re.split(r" and ", self._sql_condition, flags=re.IGNORECASE)
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
    def _u_probability_corresponding_to_exact_match(self):
        levels = self.comparison.comparison_levels

        # Find a level with a single exact match colname
        # which is equal to the tf adjustment input colname

        for level in levels:
            if not level._is_exact_match:
                continue
            colnames = level._exact_match_colnames
            if len(colnames) != 1:
                continue
            if colnames[0] == self._tf_adjustment_input_column_name.lower():
                return level.u_probability
        raise ValueError(
            "Could not find an exact match level for "
            f"{self._tf_adjustment_input_column_name}."
            "\nAn exact match level is required to make a term frequency adjustment "
            "on a comparison level that is not an exact match."
        )

    @property
    def _bayes_factor_sql(self):
        sql = f"""
        WHEN
        {self.comparison._gamma_column_name} = {self._comparison_vector_value}
        THEN cast({self._bayes_factor} as double)
        """
        return dedent(sql)

    @property
    def _tf_adjustment_sql(self):
        gamma_column_name = self.comparison._gamma_column_name
        gamma_colname_value_is_this_level = (
            f"{gamma_column_name} = {self._comparison_vector_value}"
        )

        # A tf adjustment of 1D is a multiplier of 1.0, i.e. no adjustment
        if self._comparison_vector_value == -1:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then cast(1 as double)"
        elif not self._has_tf_adjustments:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then cast(1 as double)"
        elif self._tf_adjustment_weight == 0:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then cast(1 as double)"
        elif self._is_else_level:
            sql = f"WHEN  {gamma_colname_value_is_this_level} then cast(1 as double)"
        else:
            tf_adj_col = self._tf_adjustment_input_column

            coalesce_l_r = (
                f"coalesce({tf_adj_col.tf_name_l()}, {tf_adj_col.tf_name_r()})"
            )
            coalesce_r_l = (
                f"coalesce({tf_adj_col.tf_name_r()}, {tf_adj_col.tf_name_l()})"
            )

            tf_adjustment_exists = f"{coalesce_l_r} is not null"
            u_prob_exact_match = self._u_probability_corresponding_to_exact_match

            # Using coalesce protects against one of the tf adjustments being null
            # Which would happen if the user provided their own tf adjustment table
            # That didn't contain some of the values in this data

            # In this case rather than taking the greater of the two, we take
            # whichever value exists

            if self._tf_minimum_u_value == 0.0:
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
                    AND {coalesce_l_r} > cast({self._tf_minimum_u_value} as double)
                        THEN {coalesce_l_r}
                    WHEN {coalesce_r_l}  > cast({self._tf_minimum_u_value} as double)
                        THEN {coalesce_r_l}
                    ELSE cast({self._tf_minimum_u_value} as double)
                END)
                """

            sql = f"""
            WHEN  {gamma_colname_value_is_this_level} then
                (CASE WHEN {tf_adjustment_exists}
                THEN
                POW(
                    cast({u_prob_exact_match} as double) /{divisor_sql},
                    cast({self._tf_adjustment_weight} as double)
                )
                ELSE cast(1 as double)
                END)
            """
        return dedent(sql).strip()

    def as_dict(self):
        "The minimal representation of this level to use as an input to Splink"
        output = {}

        output["sql_condition"] = self._sql_condition

        if self._level_dict.get("label_for_charts"):
            output["label_for_charts"] = self._label_for_charts

        if self._m_probability and self._m_is_trained:
            output["m_probability"] = self.m_probability

        if self._u_probability and self._u_is_trained:
            output["u_probability"] = self.u_probability

        if self._has_tf_adjustments:
            output["tf_adjustment_column"] = self._tf_adjustment_input_column.input_name
            if self._tf_adjustment_weight != 0:
                output["tf_adjustment_weight"] = self._tf_adjustment_weight

        if self._is_null_level:
            output["is_null_level"] = True
        return output

    def _as_completed_dict(self):
        comp_dict = self.as_dict()
        comp_dict["comparison_vector_value"] = self._comparison_vector_value
        return comp_dict

    @property
    def _as_detailed_record(self):
        "A detailed representation of this level to describe it in charting outputs"
        output = {}
        output["sql_condition"] = self._sql_condition
        output["label_for_charts"] = self._label_for_charts

        output["m_probability"] = self.m_probability
        output["u_probability"] = self.u_probability

        output["m_probability_description"] = self._m_probability_description
        output["u_probability_description"] = self._u_probability_description

        output["has_tf_adjustments"] = self._has_tf_adjustments
        if self._has_tf_adjustments:
            output["tf_adjustment_column"] = self._tf_adjustment_input_column.input_name
        else:
            output["tf_adjustment_column"] = None
        output["tf_adjustment_weight"] = self._tf_adjustment_weight

        output["is_null_level"] = self._is_null_level
        output["bayes_factor"] = self._bayes_factor
        output["log2_bayes_factor"] = self._log2_bayes_factor
        output["comparison_vector_value"] = self._comparison_vector_value
        output["max_comparison_vector_value"] = self.comparison._num_levels - 1
        output["bayes_factor_description"] = self._bayes_factor_description

        return output

    @property
    def _parameter_estimates_as_records(self):

        output_records = []

        cl_record = self._as_detailed_record
        trained_values = self._trained_u_probabilities + self._trained_m_probabilities
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

    def _validate(self):
        self._validate_sql()

    def _abbreviated_sql(self, cutoff=75):
        sql = self._sql_condition
        sql = (sql[:75] + "...") if len(sql) > 75 else sql
        return sql

    def __repr__(self):

        return f"<{self._human_readable_succinct}>"

    @property
    def _human_readable_succinct(self):
        sql = self._abbreviated_sql(75)
        return f"Comparison level '{self._label_for_charts}' using SQL rule: {sql}"

    @property
    def human_readable_description(self):

        input_cols = join_list_with_commas_final_and(
            [c.name(escape=False) for c in self._input_columns_used_by_sql_condition]
        )
        desc = (
            f"Comparison level: {self._label_for_charts} of {input_cols}\n"
            "Assesses similarity between pairwise comparisons of the input columns "
            f"using the following rule\n{self._sql_condition}"
        )

        return desc
