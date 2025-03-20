# Update the version of the bundled Vega JavaScript package. This script should
# be run from the root of the splink repository. Tested on Python 3.12; e.g.,

# python -m scripts.update_vega

# Note that there are a few references to the existing Vega filename in the
# repository: splink/internals/charts.py, splink/internals/cluster_studio.py,
# and splink/internals/splink_comparison_viewer.py. These must also be updated
# to reference the new Vega filename (i.e., replacing the value of
# VERSION_VEGA_EXISTING defined below with the value of VERSION_VEGA_NEW in
# those scripts).

from pathlib import Path
from urllib.request import urlretrieve

# Define the currently bundled and new versions of Vega
VERSION_VEGA_EXISTING = "5.21.0"
VERSION_VEGA_NEW = "5.31.0"

# Define filepaths of the existing and new Vega files
external_js_path = Path("splink", "internals", "files", "external_js")
vega_file_existing = external_js_path / f"vega@{VERSION_VEGA_EXISTING}"
vega_file_new = external_js_path / f"vega@{VERSION_VEGA_NEW}"

# Delete the existing Vega file
if vega_file_existing.exists():
    print(f"Deleting the existing Vega file at {str(vega_file_existing)}...")  # noqa: T201
    Path.unlink(vega_file_existing)
else:
    raise ValueError(
        "The specified existing version of Vega does not exist in the repository "
        f"at the expected location ({str(vega_file_existing)})."
    )

# Download the new file. There are a few options here. One is to download
# the package from the releases page, e.g.,
# https://github.com/vega/vega/releases/tag/v5.31.0. We could then unzip that
# folder and grab the vega.min.js file. We could also download it from, e.g.,
# https://cdn.jsdelivr.net/npm/vega@5.31. My preference, however, is to download
# the latest main-branch version of the file directly. In this approach, the
# filename isn't versioned, so you'd want to manually check that
# VERSION_VEGA_NEW is consistent with what's actually downloaded. We'll use that
# last approach here.
vega_url = (
    "https://raw.githubusercontent.com/vega/vega/refs/heads/main/docs/vega.min.js"
)
print(f"Downloading the new Vega file and saving as {str(vega_file_new)}...")  # noqa: T201
urlretrieve(vega_url, vega_file_new)

print("Vega update complete.")  # noqa: T201
