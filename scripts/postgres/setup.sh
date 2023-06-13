#!/bin/bash
# run from root

# add -d for detached mode (run in background)
docker-compose -f scripts/postgres/docker-compose.yaml up
