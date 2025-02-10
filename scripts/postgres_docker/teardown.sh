#!/bin/bash
# run from root

# remove -v (removing volume) if you want to keep data
docker compose -f scripts/postgres_docker/docker-compose.yaml down -v
