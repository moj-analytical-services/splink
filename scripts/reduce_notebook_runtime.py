import os
import json
import re


def modify_notebook(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    changed = False

    is_febrl_notebook = file_path.endswith("febrl4.ipynb") or file_path.endswith(
        "febrl3.ipynb"
    )

    # if file_path.endswith("febrl4.ipynb"):
    #     # These cells need a very high max_pairs value
    #     # otherwise you get divide by zero errors.  Easiest just to delete
    #     data["cells"] = data["cells"][:19]
    #     changed = True

    for cell in data["cells"]:
        if cell["cell_type"] == "code":
            source = cell["source"]
            new_source = []
            for line in source:
                if "splink_datasets" in line and "=" in line:
                    parts = line.split("=")
                    parts[1] = parts[1].strip() + ".head(400)"
                    new_line = " = ".join(parts) + "\n"
                    new_source.append(new_line)
                    changed = True
                elif "estimate_u_using_random_sampling(" in line:
                    new_line = (
                        re.sub(r"max_pairs=\d+(\.\d+)?[eE]\d+", "max_pairs=1e5", line)
                        + "\n"
                    )
                    new_source.append(new_line)
                    changed = True
                elif is_febrl_notebook and "ctl.name_comparison" in line:
                    new_line = (
                        line.replace("ctl.name_comparison", "cl.exact_match") + "\n"
                    )
                    new_source.append(new_line)
                    changed = True
                else:
                    new_source.append(line)

            if changed:
                cell["source"] = new_source

    if changed:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4)


def process_directory(directory):
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in [f for f in filenames if f.endswith(".ipynb")]:
            modify_notebook(os.path.join(dirpath, filename))


def main():
    base_directory = "docs/demos/examples"
    process_directory(base_directory)


if __name__ == "__main__":
    main()
