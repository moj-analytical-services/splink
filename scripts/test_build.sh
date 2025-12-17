#!/bin/bash
# run this from the repo root
set -euo pipefail

# build separate images for each distribution type
docker build -f scripts/build-test.dockerfile --progress=plain -t splink-build-test-whl .
docker build -f scripts/build-test.dockerfile --progress=plain --build-arg BUILD_TYPE=sdist -t splink-build-test-sdist .

docker run --rm --name splink-build-sdist splink-build-test-sdist
docker run --rm --name splink-build-whl splink-build-test-whl

# comment out if not needed - for file inspection
docker run --rm --name splink-build-sdist -it --entrypoint /bin/bash splink-build-test-sdist
