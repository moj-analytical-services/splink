#!/bin/bash

# Function to ensure necessary packages are installed
ensure_homebrew_packages_installed() {
    # Check if Homebrew is installed
    if ! command -v brew >/dev/null 2>&1; then
        echo "Homebrew is not installed on this system."
        echo "Please install via https://brew.sh/ before rerunning this script."
        return 1
    fi

    # Iterate over all arguments which are package names
    for package_name in "$@"; do
        if brew list --formula | grep -q "^${package_name}\$"; then
            echo "${package_name} is already installed."
        else
            echo "${package_name} is not installed. Installing ${package_name}..."
            brew install "${package_name}"
        fi
    done
}

