# Contributing to Splink

## Asking questions

If you have a question about Splink, we recommended asking on our GitHub [discussion board](https://github.com/moj-analytical-services/splink/discussions). This means that other users can benefit from the answers too! On that note, it is always worth checking if a similar question has been asked (and answered) before.

## Reporting issues

When reporting [issues](https://github.com/moj-analytical-services/splink/issues) please include as much detail as possible about your operating system, Splink version, python version and which SQL backend you are using. Whenever possible, please also include a brief, self-contained code example that demonstrates the problem. It is particularly helpful if you can look through the existing issues and provide links to any related issues.

## Contributing to documentation

Contributions to Splink are not limited to the code, feedback and input on our documentation from a user's perspective is extremely valuable - even something as small as fixing a typo. More generally, if you are interested in starting to work on Splink, documentation is a great way to get those first commits!

Behind the scenes, the Splink documentation is split into 2 parts:

- The [Tutorials](./docs/demos/00_Tutorial_Introduction.ipynb) and [Example Notebooks](./docs/examples_index.md) are stored in a separate repo - [splink_demos](https://github.com/moj-analytical-services/splink_demos)
- Everything else is stored in the Splink repo either in:
    - the [docs folder](https://github.com/moj-analytical-services/splink/tree/master/docs)
    - the Splink code itself. E.g. docstrings from [linker.py](https://github.com/moj-analytical-services/splink/blob/master/splink/linker.py) feed directly into the [Linker API docs](./docs/linker.md).

There are some user restrictions on both the splink and splink_demos repos, so to make changes to either repo you will need to create a [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) and then create a [Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) which one of the Splink dev team will review.

!!! note
    If you are looking through the docs and find an issue, hit the :material-file-edit: button on the top right corner of the page. This will take you to the underlying document in the Splink repo. This is not supported for Tutorials or Examples as they are in a separate repo.

For small changes, such as wording and typos, changes can be made directly in GitHub. However, for larger changes it may be worth cloning the relevant repo to your local machine. This way, you can [build the docs site locally](./docs/dev_guides/changing_splink/build_docs_locally.md) to check how the changes will look in the deployed doc site.


## Contributing code
Thanks for your interest in contributing code to Splink!

If this is your first time contributing to a project on GitHub, please read through our guide to contributing to numpy
If you have contributed to other projects on GitHub you can go straight to our development workflow
Either way, please be sure to follow our convention for commit messages.

If you are writing new C code, please follow the style described in doc/C_STYLE_GUIDE.

Suggested ways to work on your development version (compile and run the tests without interfering with system packages) are described in doc/source/dev/development_environment.rst.

A note on feature enhancements/API changes
If you are interested in adding a new feature to NumPy, consider submitting your feature proposal to the mailing list, which is the preferred forum for discussing new features and API changes.

### Working on an existing issue

### Working on a new issue/feature

### Philosophy