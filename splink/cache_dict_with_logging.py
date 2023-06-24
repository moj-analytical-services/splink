import logging
from collections import UserDict
from copy import copy

from .splink_dataframe import SplinkDataFrame

logger = logging.getLogger(__name__)


class CacheDictWithLogging(UserDict):
    def __init__(self):
        super().__init__()
        self.executed_queries = []
        self.queries_retrieved_from_cache = []

    def __getitem__(self, key) -> SplinkDataFrame:
        splink_dataframe = super().__getitem__(key)

        # Return a copy so that user can modify physical or templated name
        # without modifying the version in the cache
        return copy(splink_dataframe)

    def __setitem__(self, key, value):
        if not isinstance(value, SplinkDataFrame):
            raise TypeError("Cached items must be of type SplinkDataFrame")

        super().__setitem__(key, value)

        logger.log(
            1, f"Setting cache for {key}" f" with physical name {value.physical_name}"
        )

    def invalidate_cache(self):
        self.data = dict()

    def get_with_logging(self, key):
        df = self[key]
        phy_name = df.physical_name
        logger.debug(
            f"Using cache for template name {key}" f" with physical name {phy_name}"
        )
        self.queries_retrieved_from_cache.append(df)

        return df

    def reset_executed_queries_tracker(self):
        self.executed_queries = []

    def is_in_executed_queries(
        self, name_to_find, search_physical=True, search_templated=True
    ):
        names = []
        for df in self.executed_queries:
            if search_physical:
                names.append(df.physical_name)
            if search_templated:
                names.append(df.templated_name)

        return name_to_find in names

    def reset_queries_retrieved_from_cache_tracker(self):
        self.queries_retrieved_from_cache = []

    def is_in_queries_retrieved_from_cache(
        self, name_to_find, search_physical=True, search_templated=True
    ):
        names = []
        for df in self.queries_retrieved_from_cache:
            if search_physical:
                names.append(df.physical_name)
            if search_templated:
                names.append(df.templated_name)

        return name_to_find in names
