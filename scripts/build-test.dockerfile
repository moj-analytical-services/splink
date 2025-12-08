# Dockerfile for testing the package build process
# Use multi-stage build; ensures only the built artifact is used, not source files

ARG PYTHON_VERSION=3.11

# =============================================================================
# Stage 1: Build the distribution
# =============================================================================
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm AS builder

# Argument to specify build type: 'wheel' or 'sdist'
ARG BUILD_TYPE=wheel

WORKDIR /build

# copy everything, as we are trying to debug build
COPY . .

# --no-sources ensures we don't use local editable installs
RUN if [ "$BUILD_TYPE" = "sdist" ]; then \
        uv build --no-sources --sdist; \
    elif [ "$BUILD_TYPE" = "wheel" ]; then \
        uv build --no-sources --wheel; \
    else \
        echo "Invalid BUILD_TYPE: $BUILD_TYPE. Must be 'wheel' or 'sdist'" && exit 1; \
    fi

RUN echo "=== Built artifacts ===" && ls -la /build/dist/

# =============================================================================
# Stage 2: Test the distribution
# =============================================================================
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm AS tester

ARG BUILD_TYPE=wheel

WORKDIR /app

COPY --from=builder /build/dist/ /dist/

RUN if [ "$BUILD_TYPE" = "sdist" ]; then \
        uv pip install --system /dist/*.tar.gz; \
    else \
        uv pip install --system /dist/*.whl; \
    fi

# First step, simple import
RUN python -c "import splink; print(f'Successfully imported splink version: {splink.__version__}')"

RUN echo "=== Installed packages ===" && uv pip list --system
RUN python -c "import splink; print(f'Splink location: {splink.__file__}')"

# Copy test dependencies and tests
# We need pyproject.toml for pytest configuration and test dependencies
COPY pyproject.toml ./
COPY tests/ ./tests/

# Install test dependencies (pytest, pyarrow, etc.) without reinstalling splink
RUN uv pip install --system pytest pyarrow networkx rapidfuzz pytest-cov

RUN echo "=== Final installed packages ===" && uv pip list --system

RUN python -c "import splink; print(f'Splink version: {splink.__version__}'); print(f'Splink location: {splink.__file__}')"

# Default command: run duckdb tests
CMD ["python", "-m", "pytest", "-v", "--durations=10", "-m", "duckdb_only or core", "tests/"]
