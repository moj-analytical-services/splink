import os
import urllib.request

import nbformat
from bs4 import BeautifulSoup


# Function to extract only the Python code from input cells of type code
def extract_notebook_code(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    extracted_code = ""
    for cell in nb.cells:
        if cell.cell_type == "code":  # Only extract Python code
            extracted_code += cell.source + "\n\n"
    return extracted_code


# Function to traverse the directories and process .ipynb files for Python code only
def extract_and_append_notebook_code(base_dir, output_filename):
    for root, dirs, files in os.walk(base_dir):
        # Remove .ipynb_checkpoints from dirs to prevent it from being searched
        dirs[:] = [d for d in dirs if d != ".ipynb_checkpoints"]

        for file in files:
            if file.endswith(".ipynb") and not file.endswith("-checkpoint.ipynb"):
                notebook_path = os.path.join(root, file)
                # Skip files with athena or sqlite in path
                if any(
                    x in notebook_path.lower()
                    for x in [
                        "athena",
                        "sqlite",
                        "bias_eval",
                        "febrl3",
                        "deterministic",
                        "quick_and_dirty",
                        "real_time",
                        "playground",
                    ]
                ):
                    print(f"Skipping {notebook_path} due to athena/sqlite...")  # noqa: T201
                    continue

                if ".ipynb_checkpoints" not in notebook_path:
                    print(f"Processing {notebook_path}...")  # noqa: T201
                    code = extract_notebook_code(notebook_path)

                    with open(output_filename, "a", encoding="utf-8") as f:
                        f.write(f"Python code from {notebook_path}:\n")
                        f.write(code)
                        f.write("\n\n")
                else:
                    print(f"Skipping checkpoint file: {notebook_path}")  # noqa: T201


# Function to extract and append content from specified Markdown files
def extract_and_append_md_content(md_files, output_filename):
    for md_file in md_files:
        full_path = os.path.join("..", md_file.lstrip("/"))
        if os.path.exists(full_path):
            print(f"Appending content from {full_path}...")  # noqa: T201
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()

            with open(output_filename, "a", encoding="utf-8") as f:
                f.write(f"\n\nContents of {md_file}:\n")
                f.write(content)
                f.write("\n\n")
        else:
            print(f"Warning: File {full_path} not found.")  # noqa: T201


def fetch_url_content(url):
    with urllib.request.urlopen(url) as response:
        return response.read().decode("utf-8")


def fetch_article_content(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"  # NOQA: E501
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        html = response.read().decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(strip=True)


additional_instructions_to_llms = """
If the user asks for Splink code, when writing Splink code use ONLY
functions and methods which you've seen used in the context provided. Do NOT guess at
other Splink methods or functions.

If you don't know how to do something, put a comment referring them to
https://moj-analytical-services.github.io/splink

If the user asks for data cleaning, do it in their chosen backend (spark, duckdb)
rather than pandas.  In general avoid pandas unless specificaly asked for.

In most cases, you should assume the incoming data is not clean and suggest some simple
data cleaning steps.
"""

# Main execution
if __name__ == "__main__":
    output_filename = "llm_context_short.txt"
    open(output_filename, "w").close()

    # Extract and save Python code from notebooks in the specified directories
    demos_examples_dir = "../docs/demos/examples"
    demos_tutorials_dir = "../docs/demos/tutorials"

    extract_and_append_notebook_code(demos_examples_dir, output_filename)
    extract_and_append_notebook_code(demos_tutorials_dir, output_filename)

    # Append content from the index and getting started markdown files
    mds_to_append = [
        "/docs/index.md",
        "/docs/getting_started.md",
        "/docs/demos/tutorials/08_building_your_own_model.md",
    ]
    extract_and_append_md_content(mds_to_append, output_filename)

    # Add the blog articles
    blog_urls = [
        "https://www.robinlinacre.com/intro_to_probabilistic_linkage/",
        "https://www.robinlinacre.com/partial_match_weights/",
        "https://www.robinlinacre.com/m_and_u_values/",
        # "https://www.robinlinacre.com/maths_of_fellegi_sunter/",
        # "https://www.robinlinacre.com/computing_fellegi_sunter/",
        "https://www.robinlinacre.com/fellegi_sunter_accuracy/",
        "https://www.robinlinacre.com/em_intuition/",
    ]

    with open(output_filename, "a", encoding="utf-8") as f:
        f.write("\n\nBlog Articles:\n")
        for url in blog_urls:
            print(f"Fetching article from {url}...")  # noqa: T201
            content = fetch_article_content(url)
            f.write(f"\n\nArticle from {url}:\n")
            f.write(content)
            f.write("\n\n")

    # Append additional instructions to the output file
    with open(output_filename, "a", encoding="utf-8") as f:
        f.write("IMPORTANT Instructions to LLMs:")
        f.write(additional_instructions_to_llms)

    print(  # noqa: T201
        "Python code from notebooks, markdown content, Splink tips, and additional"
        " instructions extracted and saved to llm_context_short.txt"
    )
