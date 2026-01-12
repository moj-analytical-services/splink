from __future__ import annotations

import re
from pathlib import Path

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.plugins import event_priority
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page
from nbconvert import MarkdownExporter
from nbconvert.preprocessors import TagRemovePreprocessor

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
    """
    Takes markdown string content and replaces blocks such as:

    {% include-markdown "./includes/some_file.md" %}

    with the _contents_ of "docs/includes/some_file.md", or fail
    with an error if the file is not located

    If there is no such block, returns None.
    """
    if not re.search(INCLUDE_MARKDOWN_REGEX, markdown):
        return
    # if we have an include tag, replace text with file contents
    for match in re.finditer(INCLUDE_MARKDOWN_REGEX, markdown):
        text_to_replace = match.group(0)
        include_path = match.group(1)
        try:
            with open(Path("docs") / include_path) as f_inc:
                include_text = f_inc.read()
                new_text = re.sub(text_to_replace, include_text, markdown)
                # update text, in case we are iterating
                markdown = new_text
        # if we can't find include file then warn but carry on
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Couldn't find specified include file: {e}"
            ) from None
    return markdown


def re_route_links(markdown: str, page_title: str) -> str | None:
    """
    If any links are to files 'docs/foo/bar.md' (which work directly in-repo)
    reroute instead to 'foo/bar.md' (which work in structure of docs)

    To avoid false positives this is opt-in - i.e. it only works for files
    with titles as specified in tuple

    If not one of these files, or no such links, we return None
    """
    # the 'proper' way to do this would be to check if the file lives outside
    # the docs/ folder, and only adjust if so, rather relying on title
    # (which could be changed), and must be opted-into
    relevant_file_titles = ("Contributor Guide",)
    if page_title not in relevant_file_titles:
        return

    docs_folder_regex = "docs/"
    if not re.search(docs_folder_regex, markdown):
        return
    return re.sub(docs_folder_regex, "", markdown)


# hooks for use by mkdocs


# priority last - run this after any other such hooks
# this ensures we are overwriting mknotebooks config,
# not the other way round
@event_priority(-100)
def on_config(config: MkDocsConfig) -> MkDocsConfig:
    # convert ipynb to md rather than html directly
    # this ensures we render symbols such as '<' correctly
    # in codeblocks, instead of '%lt;'

    t = TagRemovePreprocessor()
    mknotebooks_config = config.get("plugins", {}).get("mknotebooks", {})
    tag_remove_configs = mknotebooks_config.config.get("tag_remove_configs", {})
    for option, setting in tag_remove_configs.items():
        setattr(t, option, set(setting))

    md_exporter = MarkdownExporter(config=config)
    md_exporter.register_preprocessor(t, enabled=True)

    # md_exporter.config["TagRemovePreprocessor"]["remove_input_tags"] = ("hideme",)
    # overwrite mknotebooks config option
    config["notebook_exporter"] = md_exporter
    return config


def on_page_markdown(
    markdown: str, page: Page, config: MkDocsConfig, files: Files
) -> str | None:
    """
    mkdocs hook to transform the raw markdown before it is sent to the renderer.

    See https://www.mkdocs.org/dev-guide/plugins/#on_page_markdown for details.
    """
    if (replaced_markdown := include_markdown(markdown)) is not None:
        return replaced_markdown
    # this only works if we don't have files that need links rewritten + includes
    # this is currently the case, so no need to worry
    if (replaced_markdown := re_route_links(markdown, page.title)) is not None:
        return replaced_markdown
    return
