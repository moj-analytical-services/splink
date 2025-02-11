#!/bin/bash
# run from root

# add -d for detached mode (run in background)
docker compose -f scripts/postgres_docker/docker-compose.yaml up
