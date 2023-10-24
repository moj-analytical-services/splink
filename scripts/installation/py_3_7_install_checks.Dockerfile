# Start from a Python 3.7.10 base image
FROM python:3.7.10

# Set the working directory in the docker image
WORKDIR /app

# Only copies what we've outlined in .dockerignore
COPY . .

# Run the bash script
CMD ["/bin/bash", "scripts/installation/test_splink_install_3.7.sh"]