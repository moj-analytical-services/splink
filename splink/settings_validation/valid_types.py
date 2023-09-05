from __future__ import annotations

import logging

from .settings_validator import SettingsValidator

logger = logging.getLogger(__name__)


class InvalidTypesAndValuesLogger(SettingsValidator):

    """Most types are checked within the settings schema validation step -
    https://github.com/moj-analytical-services/splink/blob/master/splink/validate_jsonschema.py.

    For any types that can't be checked in this step, run some quick validation checks.
    """

    def __init__(self, linker):
        self.linker = linker

    def _validate_dialect(self):
        settings_dialect = self.linker._settings_obj._sql_dialect
        linker_dialect = self.linker._sql_dialect
        if settings_dialect != linker_dialect:
            linker_type = self.linker.__class__.__name__
            raise ValueError(
                f"Incompatible SQL dialect! `settings` dictionary uses "
                f"dialect {settings_dialect}, but expecting "
                f"'{linker_dialect}' for Linker of type `{linker_type}`"
            )
