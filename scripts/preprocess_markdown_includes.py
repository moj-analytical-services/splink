# this script to be run at the root of the repo
# otherwise will not correctly traverse docs folder
import logging
import os
import re
from pathlib import Path

regex = (
    # opening tag and any whitespace
    r"{%\s*"
    # include-markdown literal and more whitespace
    r"include-markdown\s*"
    # the path in double-quotes (unvalidated)
    r"\"(.*)\""
    # more whitespace and closing tag
    r"\s*%}"
)


for root, _dirs, files in os.walk("docs"):
    for file in files:
        # only process md files
        if re.fullmatch(r".*\.md", file):
            with open(Path(root) / file, "r") as f:
                current_text = f.read()
            if re.search(regex, current_text):
                # if we have an include tag, replace text and overwrite
                for match in re.finditer(regex, current_text):
                    text_to_replace = match.group(0)
                    include_path = match.group(1)
                    try:
                        with open(Path(root) / include_path) as f_inc:
                            include_text = f_inc.read()
                            new_text = re.sub(
                                text_to_replace, include_text, current_text
                            )
                            with open(Path(root) / file, "w") as f:
                                f.write(new_text)
                            # update text, in case we are iterating
                            current_text = new_text
                    # if we can't find include file then warn but carry on
                    except FileNotFoundError as e:
                        logging.warning(f"Couldn't find specified include file: {e}")

# nasty hack:
# also adjust CONTRIBUTING.md links - mismatch between links as on github and
# where it fits in folder hierarchy of docs
with open(Path(".") / "CONTRIBUTING.md", "r") as f:
    contributing_text = f.read()
# in docs CONTRIBUTING.md 'thinks' it's already in docs/, so remove that level from
# relative links
new_text = re.sub(
    "docs/", "", contributing_text
)
with open(Path(".") / "CONTRIBUTING.md", "w") as f:
    f.write(new_text)
