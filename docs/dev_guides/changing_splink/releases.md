# Releasing a new version of Splink

Splink is regularly updated with releases to add new features or bug fixes to the package.

Below are the steps for releasing a new version of Splink:

1. Update [**project.toml**](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) and [**__init__.py**](https://github.com/moj-analytical-services/splink/blob/master/splink/__init__.py) with the latest version.
2. Generate a new release in the [releases section of the repo](https://github.com/moj-analytical-services/splink/releases)

Including:

- Choosing a new release tag (which matches your updates to [**project.toml**](https://github.com/moj-analytical-services/splink/blob/master/pyproject.toml) and [**__init__.py**](https://github.com/moj-analytical-services/splink/blob/master/splink/__init__.py)

![](https://raw.githubusercontent.com/moj-analytical-services/splink/release_guide/docs/img/releases-tag.png))

- Generating release notes. This can be done automatically by pressing the 
![](https://raw.githubusercontent.com/moj-analytical-services/splink/release_guide/docs/img/releases-notes_button.png) button. 

This will give you release notes based off the Pull Requests which have been merged since the last release.

For example
![](https://raw.githubusercontent.com/moj-analytical-services/splink/release_guide/docs/img/releases-notes.png)

- Publish as the latest release

![](https://raw.githubusercontent.com/moj-analytical-services/splink/release_guide/docs/img/releases-publish.png)


Now your release should be published to [pypi](https://pypi.org/project/splink/#history).


