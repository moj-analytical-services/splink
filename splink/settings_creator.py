from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Union

from .blocking_rule_creator import BlockingRuleCreator
from .blocking_rule_library import CustomRule
from .comparison_creator import ComparisonCreator
from .comparison_library import CustomComparison
from .settings import Settings


def to_comparison_creator(comparison_creator):
    if isinstance(comparison_creator, dict):
        return CustomComparison(**comparison_creator)
    return comparison_creator


def to_blocking_rule_creator(blocking_rule_creator):
    if isinstance(blocking_rule_creator, dict):
        return CustomRule(**blocking_rule_creator)
    if isinstance(blocking_rule_creator, str):
        return CustomRule(blocking_rule_creator)
    return blocking_rule_creator


@dataclass
class SettingsCreator:
    """
    Non-dialected version of Settings.
    Responsible for authoring Settings, but not implementing anything
    """

    link_type: str
    # TODO: make this compulsory once we farm more stuff out of linker
    comparisons: List[ComparisonCreator | dict] = field(default_factory=list)
    blocking_rules_to_generate_predictions: List[BlockingRuleCreator | dict] = field(
        default_factory=list
    )

    probability_two_random_records_match: float = 0.0001
    em_convergence: float = 0.0001
    max_iterations: int = 25

    retain_matching_columns: bool = True
    retain_intermediate_calculation_columns: bool = False
    additional_columns_to_retain: List[str] = field(default_factory=list)

    unique_id_column_name: str = "unique_id"
    source_dataset_column_name: str = "source_dataset"
    bayes_factor_column_prefix: str = "bf_"
    term_frequency_adjustment_column_prefix: str = "tf_"
    comparison_vector_value_column_prefix: str = "gamma_"

    linker_uid: str = None

    def _as_naive_dict(self) -> dict:
        """
        Returns this class as a naive dict.
        Naive in the sense that we do not process the attributes in any way.

        In particular blocking rules and comparisons could be dicts _or_ creator objects
        """
        return asdict(self)

    def _as_creator_dict(self) -> dict:
        """
        Returns class as a dict where we have converted any sub-dicts into
        'creator' types
        """
        creator_dict = self._as_naive_dict()
        # we adjust dict to ensure that comparisons + blocking rules are
        # consistently of creatore types
        creator_dict["comparisons"] = [
            to_comparison_creator(comparison_creator)
            for comparison_creator in creator_dict["comparisons"]
        ]
        creator_dict["blocking_rules_to_generate_predictions"] = [
            to_blocking_rule_creator(blocking_rule_creator)
            for blocking_rule_creator in creator_dict[
                "blocking_rules_to_generate_predictions"
            ]
        ]
        return creator_dict

    def create_settings_dict(self, sql_dialect_str: str) -> dict:
        creator_dict = self._as_creator_dict()
        # then we process 'creator' types into dialected dicts
        creator_dict["comparisons"] = [
            comparison_creator.create_comparison_dict(sql_dialect_str)
            for comparison_creator in creator_dict["comparisons"]
        ]
        creator_dict["blocking_rules_to_generate_predictions"] = [
            blocking_rule_creator.create_blocking_rule_dict(sql_dialect_str)
            for blocking_rule_creator in creator_dict[
                "blocking_rules_to_generate_predictions"
            ]
        ]
        return creator_dict

    def get_settings(self, sql_dialect_str) -> Settings:
        creator_dict = self._as_creator_dict()
        # then we process 'creator' types into dialected concrete types
        creator_dict["comparisons"] = [
            comparison_creator.get_comparison(sql_dialect_str)
            for comparison_creator in creator_dict["comparisons"]
        ]
        creator_dict["blocking_rules_to_generate_predictions"] = [
            blocking_rule_creator.get_blocking_rule(sql_dialect_str)
            for blocking_rule_creator in creator_dict[
                "blocking_rules_to_generate_predictions"
            ]
        ]
        return Settings(**creator_dict, sql_dialect=sql_dialect_str)

    @classmethod
    def from_path_or_dict(cls, path_or_dict: Union[Path, str, dict]) -> SettingsCreator:

        if isinstance(path_or_dict, (str, Path)):
            settings_path = Path(path_or_dict)
            if settings_path.is_file():

                settings_dict = json.loads(settings_path.read_text())

                # TODO: remove this once we have sorted spec
                for br in settings_dict["blocking_rules_to_generate_predictions"]:
                    if isinstance(br, dict):
                        if "sql_dialect" in br:
                            del br["sql_dialect"]

        elif isinstance(path_or_dict, dict):
            settings_dict = deepcopy(path_or_dict)
        else:
            raise ValueError(f"Path {settings_path} does not point to a valid file.")

        # TODO: need to figure out how this flows with validation
        # for now we instantiate all the correct types before the validator sees it
        # self._validate_settings_components(settings)

        # TODO: should SettingsCreator deal with the logic of sql_dialect being
        # set?
        settings_dict.pop("sql_dialect", None)
        return SettingsCreator(**settings_dict)