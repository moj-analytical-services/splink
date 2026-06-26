from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from splink.internals.linker import Linker


class LinkerMisc:
    """Miscellaneous methods on the linker that don't fit into other categories.
    Accessed via `linker.misc`.
    """

    def __init__(self, linker: Linker):
        self._linker = linker

    def save_model_to_json(
        self, out_path: str | None = None, overwrite: bool = False
    ) -> dict[str, Any]:
        """Save the configuration and parameters of the linkage model to a `.json` file.

        The model can later be loaded into a new linker using
        `Linker(df, settings="path/to/model.json")`.

        The settings dict is also returned in case you want to save it a different way.

        Examples:
            ```py
            linker.misc.save_model_to_json("my_settings.json", overwrite=True)
            ```
        Args:
            out_path (str, optional): File path for json file. If None, don't save to
                file. Defaults to None.
            overwrite (bool, optional): Overwrite if already exists? Defaults to False.

        Returns:
            dict: The settings as a dictionary.
        """
        model_dict = self._linker._settings_obj.as_dict()
        if out_path:
            if os.path.isfile(out_path) and not overwrite:
                raise ValueError(
                    f"The path {out_path} already exists. Please provide a different "
                    "path or set overwrite=True"
                )
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(model_dict, f, indent=4)
        return model_dict

    def query_sql(self, sql, output_type="splink_df"):
        """
        Run a SQL query against your backend database and return
        the resulting output.

        Examples:
            ```py
            linker = Linker(df, settings)
            df_predict = linker.inference.predict()
            linker.misc.query_sql(f"select * from {df_predict.physical_name} limit 10")
            ```

        Args:
            sql (str): The SQL to be queried.
            output_type (str): One of splink_df/splinkdf or pandas.
                This determines the type of table that your results are output in.
        """
        return self._linker._db_api.query_sql(sql, output_type)
