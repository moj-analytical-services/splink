# Releasing a new version of Splink

Splink is regularly updated with releases to add new features or bug fixes to the package.

Below are the steps for releasing a new version of Splink:

1. On a new branch, update [**pyproject.toml**](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) and [**__init__.py**](https://github.com/moj-analytical-services/splink/blob/master/splink/__init__.py) with the latest version.
2. Update [**CHANGELOG.md**](https://github.com/moj-analytical-services/splink/blob/master/CHANGELOG.md). This consists of adding a heading for the new release below the 'Unreleased' heading, with the new version and date. Additionally the links at the bottom of the file for 'unreleased' and the new version should be updated.
3. Open a pull request to merge the new branch with the master branch (the base branch).
4. Once the pull request has been approved, merge the changes and generate a new release in the [releases section of the repo](https://github.com/moj-analytical-services/splink/releases), including:

- Choosing a new release tag (which matches your updates to [**pyproject.toml**](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) and [**__init__.py**](https://github.com/moj-analytical-services/splink/blob/master/splink/__init__.py)). Ensure that your release tag follows [semantic versioning](https://docs.npmjs.com/about-semantic-versioning). The target branch should be set to master.


![](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/releases/tag.png))

- Generating release notes. This can be done automatically by pressing the 
![](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/releases/notes_button.png) button. 

This will give you release notes based off the Pull Requests which have been merged since the last release.

For example
![](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/releases/notes.png)

- Publish as the latest release

![](https://raw.githubusercontent.com/moj-analytical-services/splink/master/docs/img/releases/publish.png)


Now your release should be published to [PyPI](https://pypi.org/project/splink/#history).


