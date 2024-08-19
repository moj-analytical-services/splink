from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def extract_sql_dialect_from_cll(cll):
    if isinstance(cll, dict):
        return cll.get("sql_dialect")
    else:
        return getattr(cll, "_sql_dialect", None)


def _validate_dialect(
    settings_dialect: str, linker_dialect: str, linker_type: str
) -> None:
    # settings_dialect = self.linker._settings_obj._sql_dialect
    # linker_dialect = self.linker._sql_dialect
    # linker_type = self.linker.__class__.__name__
    if settings_dialect != linker_dialect:
        raise ValueError(
            f"Incompatible SQL dialect! `settings` dictionary uses "
            f"dialect {settings_dialect}, but expecting "
            f"'{linker_dialect}' for Linker of type `{linker_type}`"
        )
