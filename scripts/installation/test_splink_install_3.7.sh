#!/bin/bash

# The python version is set within the docker image

# Set which backends you want to install and test
backends=("" "postgres" "spark" "athena")
# backends=("athena")

# Print the current Python version
python3 --version

# Loop over each backend and source the script with the appropriate argument
for backend in "${backends[@]}"
do
    source ./scripts/installation/test_poetry_build.sh "[$backend]"
done