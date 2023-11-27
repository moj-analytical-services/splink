import os
import json
import re


def modify_notebook(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    changed = False
    for cell in data["cells"]:
        if cell["cell_type"] == "code":
            source = cell["source"]
            new_source = []
            for line in source:
                if "splink_datasets" in line and "=" in line:
                    parts = line.split("=")
                    parts[1] = parts[1].strip() + ".head(100)"
                    new_line = " = ".join(parts) + "\n"
                    new_source.append(new_line)
                    changed = True
                elif "estimate_u_using_random_sampling(" in line:
                    # Use regular expression to replace max_pairs value
                    new_line = (
                        re.sub(r"max_pairs=\d+(\.\d+)?[eE]\d+", "max_pairs=1e6", line)
                        + "\n"
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
