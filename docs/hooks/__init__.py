from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page

INCLUDE_MARKDOWN_REGEX = (
    # opening tag and any whitespace
    r"{%\s*"
    # include-markdown literal and more whitespace
    r"include-markdown\s*"
    # the path in double-quotes (unvalidated)
    r"\"(.*)\""
    # more whitespace and closing tag
    r"\s*%}"
)

def include_markdown(markdown: str) -> str | None:
    if not re.search(INCLUDE_MARKDOWN_REGEX, markdown):
        return
    # if we have an include tag, replace text with file contents
    for match in re.finditer(INCLUDE_MARKDOWN_REGEX, markdown):
        text_to_replace = match.group(0)
        include_path = match.group(1)
        try:
            with open(Path("docs") / include_path) as f_inc:
                include_text = f_inc.read()
                new_text = re.sub(
                    text_to_replace, include_text, markdown
                )
                # update text, in case we are iterating
                markdown = new_text
        # if we can't find include file then warn but carry on
        except FileNotFoundError as e:
            logging.warning(f"Couldn't find specified include file: {e}")
    return markdown


# def re_route_links(markdown: str) -> str | None
# nasty hack:
# also adjust CONTRIBUTING.md links - mismatch between links as on github and
# where it fits in folder hierarchy of docs
with open(Path(".") / "CONTRIBUTING.md", "r") as f:
    contributing_text = f.read()
# in docs CONTRIBUTING.md 'thinks' it's already in docs/, so remove that level from
# relative links
new_text = re.sub("docs/", "", contributing_text)
with open(Path(".") / "CONTRIBUTING.md", "w") as f:
    f.write(new_text)


def on_page_markdown_hook(
    markdown: str, page: Page, config: MkDocsConfig, files: Files
) -> str | None:
    if (replaced_markdown := include_markdown(markdown)) is not None:
        return replaced_markdown
    return
