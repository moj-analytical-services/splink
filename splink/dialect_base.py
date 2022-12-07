# defines default values for dialect-dependent properties
class DialectBase:
    @property
    def _sql_dialect(self):
        raise NotImplementedError("No SQL dialect specified")

    @property
    def _levenshtein_name(self):
        return "levenshtein"

    @property
    def _jaro_winkler_name(self):
        return "jaro_winkler"

    @property
    def _jaccard_name(self):
        return "jaccard"
