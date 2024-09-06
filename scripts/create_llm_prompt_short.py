import os

import nbformat


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


# Main execution
if __name__ == "__main__":
    output_filename = "llm_context_short.txt"

    # Extract and save Python code from notebooks in the specified directories
    demos_examples_dir = "../docs/demos/examples"
    demos_tutorials_dir = "../docs/demos/tutorials"

    extract_and_append_notebook_code(demos_examples_dir, output_filename)
    extract_and_append_notebook_code(demos_tutorials_dir, output_filename)

    # Append content from the index and getting started markdown files
    mds_to_append = [
        "/docs/index.md",
        "/docs/getting_started.md",
    ]
    extract_and_append_md_content(mds_to_append, output_filename)

    print(  # noqa: T201
        "Python code from notebooks and markdown content extracted and saved to "
        "extracted_python_code_and_markdown.txt"
    )
