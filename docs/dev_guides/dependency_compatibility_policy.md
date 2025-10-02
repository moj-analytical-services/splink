This page highlights the importance of package versioning and proposes that we use a "sunsetting" strategy for updating our support python and dependency versions as they reach end-of-life.

Additionally, it lays out some rough guidelines for us to follow when addresses future package conflicts and issues arises from antiquated dependency versions.

<hr>

## Package Versioning Policy

Monitoring package versioning within Splink is important. It ensures that the project can be used by as wide a group of individuals as possible, without wreaking havoc on our issues log.

Below is a rough summary of versioning and some complimentary guidelines detailing how we should look to deal with dependency management going forward.

### Benefits to Effective Versioning

Effective versioning is crucial for ensuring Splink's compatibility across diverse technical ecosystems and seamless integration with various Python versions and cloud tools. Key advantages include:

* Faster dependency resolution with `uv lock`.
* Reduces dependency conflicts across systems.


### Versioning Guidance

#### Establish Minimum Supported Versions

* **Align with Python Versions**: Select the minimum required versions for dependencies based on the earliest version of Python we plan to support. This approach is aligned with our policy on [**Sunsetting End-of-Life Python Versions**](#sunsetting-end-of-life-python-versions), ensuring Splink remains compatible across a broad spectrum of environments.
* **Document Reasons**:  Where appropriate, clearly document why specific versions are chosen as minimums, including any critical features or bug fixes that dictate these choices. We should look to do this in pull requests implementing the change and as comments in [`pyproject.toml`](https://github.com/moj-analytical-services/splink/blob/9499e4ee93e6157fcc6f228b60592a7cf97bb6a0/pyproject.toml#L143). Doing so allows us to easily track versioning decisions.

#### Prefer Open Version Constraints

* **Use Open Upper Bounds**: Wherever feasible, avoid setting an upper version limit for a dependency. This reduces compatibility conflicts with external packages and allows the user to decide their versioning strategy at the application level.
* **Monitor Compatibility**: Actively monitor the development of our core dependencies to anticipate significant updates (such as new major versions) that might necessitate code changes. Within Splink, this is particularly relevant for both SQLGlot and :simple-duckdb: DuckDB, that (semi)frequently release new, breaking changes.

#### Compatibility Checks

* **Automated Testing**: Use Continuous Integration (CI) to help test the latest python and package versions. This helps identify compatibility issues early.
* **Matrix Testing**: Test against a matrix of dependencies or python versions to ensure broad compatibility. [pytest_run_tests_with_cache.yml](https://github.com/moj-analytical-services/splink/blob/master/.github/workflows/pytest_duckdb.yml) is currently our broad compatibility check for supported versions of python.

#### Handling Breaking Changes

* **Temporary Version Pinning for Major Changes**: In cases where a dependency introduces breaking changes that we cannot immediately accommodate, we should look to temporarily pin to a specific version or version range until we have an opportunity to update Splink.
* **Adaptive Code Changes**: When feasible, adapt code to be compatible with new major versions of dependencies. This may include conditional logic to handle differences across versions. An example of this can be found within [`input_column.py`](https://github.com/moj-analytical-services/splink/blob/d15c7adb8776260445615f7934c86e819b998c99/splink/input_column.py#L338), where we adjust how column identifiers are extracted from SQLGlot based on its version.

#### Documentation and Communication

* **Clear Documentation**: Clearly log installation instructions within the [Getting Started](https://moj-analytical-services.github.io/splink/getting_started.html#install) section of our documentation. This should cover not only standard installation procedures but also specialised instructions, for instance, installing a [:simple-duckdb:-less version of Splink](https://github.com/moj-analytical-services/splink/pull/1244), for locked down environments.
* **Log Dependency Changes in the CHANGELOG**: Where dependencies are adjusted, ensure that changes are logged within [`CHANGELOG.md`](https://github.com/moj-analytical-services/splink/blob/master/CHANGELOG.md). This can help simplify debugging and creates a guide that can be easily referenced.

#### User Support and Feedback

* **Issue Tracking**: Actively track and address issues related to dependency compatibility. Where users are having issues, have them report their package versions through either `pip freeze` or `pip-chill`, so we can more easily identify what may have caused the problem.
* **Feedback Loops**: Encourage feedback from users regarding compatibility and dependency issues. Streamline the reporting process in our issues log.

<hr>

## Sunsetting End-of-Life Python Versions

In alignment with the Python community's practices, we are phasing out support for Python versions that have hit [end-of-life](https://devguide.python.org/versions/) and are no longer maintained by the core Python development team. This decision ensures that Splink remains secure, efficient, and up-to-date with the latest Python features and improvements.

Our approach mirrors that of key package maintainers, such as the developers behind NumPy. The NumPy developers have kindly pulled together [**NEP 29**](https://scikit-hep.org/supported-python-versions), their guidelines for python version support. This outlines a recommended framework for the deprecation of outdated Python versions.

### Benefits of Discontinuing Support for Older Python Versions:

* Enhanced Tooling: Embracing newer versions enables the use of advanced Python features, as we no longer need to use only features available in outdated python versions. See [the new features in 3.9](https://docs.python.org/3.9/whatsnew/3.9.html) that we can use after dropping support for 3.8.
* Fewer Dependabot Alerts: Transitioning away from older Python versions reduces the volume of alerts associated with legacy package dependencies.
* Minimised Package Conflicts: Updating python decreases the necessity for [makeshift solutions](https://github.com/moj-analytical-services/splink/blob/9499e4ee93e6157fcc6f228b60592a7cf97bb6a0/pyproject.toml#L26) to resolve dependency issues with our core dependencies, fostering a smoother integration with tools like Poetry.

For a comprehensive rationale behind upgrading, the article ["It's time to stop using python 3.8"](https://pythonspeed.com/articles/stop-using-python-3.8/) offers an insightful summary.

### Implementation Timeline:
The cessation of support for major Python versions post-end-of-life will not be immediate but will instead be phased in gradually over the months following their official end-of-life designation.

Proposed Workflow for Sunsetting Major Python Versions:

1. **Initial Grace Period**: We propose a waiting period of approximately six months post-end-of-life before initiating the upgrade process. This interval:
    * Mitigates potential complications arising from system-wide Python updates across major cloud distributors and network administrators.
    * Provides a window to inform users about the impending deprecation of older versions.
2. **Following the Grace Period**:
    * Ensure the upgrade process is seamless and devoid of critical issues, leveraging the backward compatibility strengths of newer Python versions.
    * Address any bugs discovered during the upgrade process.
    * Update [`pyproject.toml`](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) accordingly. Pull requests updating our supported versions should be clearly marked with the `[DEPENDENCIES]` tag and `python_version_update` label for straightforward tracking.

### Python's Development Cycle:

A comprehensive summary of Python's development cycle is available on the [Python Developer's Guide](https://devguide.python.org/versions/). This includes a chart outlining the full release cycle up to 2029:

![](../img/dependency_management/python_release_cycle.png){width="800"}

As it stands, support for Python 3.9 will officially end in October of 2025. Following an initial grace period of around six months, we will then look to phase out support.

We will look to regularly review this page and update Splink's dependencies accordingly.
