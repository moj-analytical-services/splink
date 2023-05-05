# defines default values for dialect-dependent properties
# used by comparisons and comparison levels
# individual dialects subclass this and overwrite relevant properties
# dialect-specific comparisons and comparison levels then inherit from
# the relevant subclass in splink/{dialect}/{dialect}_base.py
class DialectBase:
    @property
    def _sql_dialect(self):
        raise NotImplementedError("No SQL dialect specified")

    @property
    def _size_array_intersect_function(self):
        raise NotImplementedError(
            f"Size array intersect function not defined for "
            f"object of type {type(self)}.  "
            f"Have you remembered to use dialect-specific "
            f"comparisons/comparison levels?"
        )

    @property
    def _datediff_function(self):
        raise NotImplementedError(
            f"Datediff function not defined for "
            f"object of type {type(self)}.  "
            f"Have you remembered to use dialect-specific "
            f"comparisons/comparison levels?"
        )

    @property
    def _levenshtein_name(self):
        return "levenshtein"

    @property
    def _jaro_name(self):
        return "jaro"

    @property
    def _jaro_winkler_name(self):
        return "jaro_winkler"

    @property
    def _jaccard_name(self):
        return "jaccard"
