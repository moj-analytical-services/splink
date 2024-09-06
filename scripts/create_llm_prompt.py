import inspect
import logging
import os

import nbformat

import splink.blocking_analysis as blocking_analysis
import splink.comparison_level_library as cll
import splink.comparison_library as cl
import splink.exploratory as exploratory
from splink import DuckDBAPI, Linker, block_on, splink_datasets
from splink.internals.settings_creator import SettingsCreator


# Function to extract content from input cells of type Python or Markdown
def extract_notebook_content(notebook_path):
    with open(notebook_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)

    extracted_content = ""
    for cell in nb.cells:
        if cell.cell_type in ["code", "markdown"]:
            extracted_content += cell.source + "\n\n"
    return extracted_content


# Function to traverse the directories and process .ipynb files
def extract_and_append_notebook_content(base_dir, docstring_filename):
    for root, dirs, files in os.walk(base_dir):
        # Remove .ipynb_checkpoints from dirs to prevent it from being searched
        dirs[:] = [d for d in dirs if d != ".ipynb_checkpoints"]

        for file in files:
            if file.endswith(".ipynb") and not file.endswith("-checkpoint.ipynb"):
                notebook_path = os.path.join(root, file)
                if ".ipynb_checkpoints" not in notebook_path:
                    print(f"Processing {notebook_path}...")
                    content = extract_notebook_content(notebook_path)

                    with open(docstring_filename, "a", encoding="utf-8") as f:
                        f.write(f"Contents of {notebook_path}:\n")
                        f.write(content)
                        f.write("\n\n")
                else:
                    print(f"Skipping checkpoint file: {notebook_path}")


# Mock objects for instantiation, replace with real ones if available
mock_settings = SettingsCreator(
    link_type="dedupe_only",
    comparisons=[
        cl.ExactMatch("first_name"),
        cl.ExactMatch("surname"),
        cl.ExactMatch("dob"),
    ],
)  # or pass a real settings object
mock_db_api = DuckDBAPI()  # or pass a real database API subclass instance

logging.getLogger("splink").setLevel(logging.ERROR)

linker = Linker(
    input_table_or_tables=splink_datasets.fake_1000,  # replace with real input
    settings=mock_settings,
    db_api=mock_db_api,
    set_up_basic_logging=False,
)

# Run inference to get SplinkDataFrame object
splink_data_frame = linker.inference.predict()

# Run training session to get EM training session object
em_training_session = (
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("first_name")
    )
)


# Function to extract all public methods from a class instance
def extract_instance_method_docstrings(instance):
    docstrings = {}
    for name, member in inspect.getmembers(instance):
        if callable(member) and not name.startswith("_"):  # Ignore private methods
            full_method_name = f"{instance.__class__.__name__}.{name}"
            docstring = inspect.getdoc(member)
            docstrings[full_method_name] = (
                docstring if docstring else "No docstring available"
            )
    return docstrings


# Function to extract all public methods from the specified submodules of the Linker class instance
def extract_method_docstrings(linker_instance, submodule_list):
    docstrings = {}
    for submodule_name in submodule_list:
        submodule_obj = getattr(linker_instance, submodule_name)
        for name, member in inspect.getmembers(submodule_obj):
            if callable(member) and not name.startswith("_"):  # Ignore private methods
                full_method_name = (
                    f"{linker_instance.__class__.__name__}.{submodule_name}.{name}"
                )
                docstring = inspect.getdoc(member)
                docstrings[full_method_name] = (
                    docstring if docstring else "No docstring available"
                )
    return docstrings


# Function to extract docstrings from the __init__ methods of all public classes in a module
def extract_class_docstrings_from_module(module):
    docstrings = {}
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if not name.startswith("_"):  # Ignore private classes
            init_docstring = inspect.getdoc(obj.__init__)
            docstrings[name] = (
                init_docstring if init_docstring else "No docstring available"
            )
    return docstrings


# Function to extract docstrings from specified functions in a module
def extract_function_docstrings(module, function_names):
    docstrings = {}
    for func_name in function_names:
        func_obj = getattr(module, func_name, None)
        if callable(func_obj):
            docstring = inspect.getdoc(func_obj)
            docstrings[func_name] = docstring if docstring else "No docstring available"
    return docstrings


# Function to extract all public functions from a module
def extract_all_function_docstrings_from_module(module):
    docstrings = {}
    for name, obj in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("_"):  # Ignore private functions
            docstring = inspect.getdoc(obj)
            docstrings[name] = docstring if docstring else "No docstring available"
    return docstrings


def save_docstrings_with_append(
    docstrings, docstring_filename="llm_context_long.txt", append_filenames=None
):
    append_content = ""
    if append_filenames:
        for filename in append_filenames:
            with open(filename, "r", encoding="utf-8") as append_file:
                append_content += append_file.read() + "\n\n"

    with open(docstring_filename, "w", encoding="utf-8") as file:
        # Organize docstrings into sections
        sections = {
            "Linker Methods": [],
            "Comparison Methods": [],
            "Comparison Level Methods": [],
            "Exploratory Functions": [],
            "Blocking Functions": [],
            "SplinkDataFrame Methods": [],
            "EMTrainingSession Methods": [],
            "Other Methods": [],
        }

        for method_path, docstring in docstrings.items():
            if method_path.startswith("Linker."):
                sections["Linker Methods"].append((method_path, docstring))
            elif method_path in comparison_docstrings:
                sections["Comparison Methods"].append((method_path, docstring))
            elif method_path in comparison_level_docstrings:
                sections["Comparison Level Methods"].append((method_path, docstring))
            elif method_path in exploratory_docstrings:
                sections["Exploratory Functions"].append((method_path, docstring))
            elif (
                method_path in blocking_analysis_docstrings or method_path == "block_on"
            ):
                sections["Blocking Functions"].append((method_path, docstring))
            elif method_path.startswith("DuckDBDataFrame."):
                sections["SplinkDataFrame Methods"].append((method_path, docstring))
            elif method_path.startswith("EMTrainingSession."):
                sections["EMTrainingSession Methods"].append((method_path, docstring))
            else:
                sections["Other Methods"].append((method_path, docstring))

        # Write sections to file and print to console
        for section_name, section_docstrings in sections.items():
            if section_docstrings:
                file.write(f"## {section_name}\n\n")
                print(f"\nOutputting docstrings for: {section_name}")
                for method_path, docstring in section_docstrings:
                    print(f"  {method_path}")
                    file.write(f"{method_path}:\n")
                    file.write(f"{docstring}\n\n")
                file.write("\n")

        if append_content:
            file.write("\n\n")
            file.write(append_content)

    print(
        "\nDocstrings extracted, saved, and organized into sections in llm_context_long.txt"
    )


# Function to extract and append content from specified Markdown files
def extract_and_append_md_content(md_files, docstring_filename):
    for md_file in md_files:
        full_path = os.path.join("..", md_file.lstrip("/"))
        if os.path.exists(full_path):
            print(f"Appending content from {full_path}...")
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()

            with open(docstring_filename, "a", encoding="utf-8") as f:
                f.write(f"\n\nContents of {md_file}:\n")
                f.write(content)
                f.write("\n\n")
        else:
            print(f"Warning: File {full_path} not found.")


# Main execution
if __name__ == "__main__":
    # Extract docstrings for all public methods in specified Linker submodules
    linker_docstrings = extract_method_docstrings(
        linker,
        [
            "inference",
            "training",
            "visualisations",
            "clustering",
            "evaluation",
            "table_management",
            "misc",
        ],
    )

    # Extract docstrings for all public classes in comparison_library
    comparison_docstrings = extract_class_docstrings_from_module(cl)

    # Extract docstrings for all public classes in comparison_level_library
    comparison_level_docstrings = extract_class_docstrings_from_module(cll)

    # Extract docstrings for specified functions in the exploratory module
    exploratory_functions = ["completeness_chart", "profile_columns"]
    exploratory_docstrings = extract_function_docstrings(
        exploratory, exploratory_functions
    )

    # Extract docstring for block_on function
    block_on_docstring = {"block_on": inspect.getdoc(block_on)}

    # Extract docstrings for all public functions in blocking_analysis
    blocking_analysis_docstrings = extract_all_function_docstrings_from_module(
        blocking_analysis
    )

    # Extract docstrings for all public methods of the SplinkDataFrame instance
    splink_data_frame_docstrings = extract_instance_method_docstrings(splink_data_frame)

    # Extract docstrings for all public methods of the EM training session instance
    em_training_session_docstrings = extract_instance_method_docstrings(
        em_training_session
    )

    # Combine all sets of docstrings
    all_docstrings = {
        **linker_docstrings,
        **comparison_docstrings,
        **comparison_level_docstrings,
        **exploratory_docstrings,
        **block_on_docstring,
        **blocking_analysis_docstrings,
        **splink_data_frame_docstrings,  # Include SplinkDataFrame methods docstrings
        **em_training_session_docstrings,  # Include EM training session methods docstrings
    }

    # Save to file and append the contents of the settings_dict_guide.md and datasets.md
    print("Extracting and saving docstrings...")
    save_docstrings_with_append(
        all_docstrings,
        "llm_context_long.txt",
        append_filenames=[
            "../docs/api_docs/settings_dict_guide.md",
            "../docs/api_docs/datasets.md",
        ],
    )

    # Add new part to extract and append content from notebooks
    demos_examples_dir = "../docs/demos/examples"
    demos_tutorials_dir = "../docs/demos/tutorials"

    extract_and_append_notebook_content(demos_examples_dir, "llm_context_long.txt")
    extract_and_append_notebook_content(demos_tutorials_dir, "llm_context_long.txt")

    # New part: Append content from specified Markdown files
    mds_to_append = [
        "/docs/index.md",
        "/docs/getting_started.md",
        "/docs/topic_guides/splink_fundamentals/settings.md",
        "/docs/topic_guides/splink_fundamentals/backends/backends.md",
        "/docs/topic_guides/blocking/blocking_rules.md",
        "/docs/topic_guides/blocking/model_training.md",
        "/docs/topic_guides/blocking/performance.md",
        "/docs/topic_guides/comparisons/comparisons_and_comparison_levels.md",
        "/docs/topic_guides/performance/performance_evaluation.md",
        "/docs/api_docs/settings_dict_guide.md",
    ]
    extract_and_append_md_content(mds_to_append, "llm_context_long.txt")

    print(
        "Docstrings extracted, saved, and all specified content appended to llm_context_long.txt"
    )
