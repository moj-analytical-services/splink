# add default marker to all tests - this flag is on by default
# set in pyproject.toml to aid testing tests/
def pytest_collection_modifyitems(items, config):
    for item in items:
        item.add_marker("default")
