# Contributing to Splink

Contributing to an open source project takes many forms. Below are some of the ways you can contribute to Splink!

## Asking questions

If you have a question about Splink, we recommended asking on our GitHub [discussion board](https://github.com/moj-analytical-services/splink/discussions). This means that other users can benefit from the answers too! On that note, it is always worth checking if a similar question has been asked (and answered) before.

## Reporting issues

Is something broken? Or not acting how you would expect? Are we missing a feature that would make your life easier? We want to know about it!

When reporting [issues](https://github.com/moj-analytical-services/splink/issues) please include as much detail as possible about your operating system, Splink version, python version and which SQL backend you are using. Whenever possible, please also include a brief, self-contained code example that demonstrates the problem. It is particularly helpful if you can look through the existing issues and provide links to any related issues.

## Contributing to documentation

Contributions to Splink are not limited to the code. Feedback and input on our documentation from a user's perspective is extremely valuable - even something as small as fixing a typo. More generally, if you are interested in starting to work on Splink, documentation is a great way to get those first commits!

Behind the scenes, the Splink documentation is split into 2 parts:

- The [Tutorials](./docs/demos/00_Tutorial_Introduction.ipynb) and [Example Notebooks](./docs/examples_index.md) are stored in a separate repo - [splink_demos](https://github.com/moj-analytical-services/splink_demos)
- Everything else is stored in the Splink repo either in:
    - the [docs folder](https://github.com/moj-analytical-services/splink/tree/master/docs)
    - the Splink code itself. E.g. docstrings from [linker.py](https://github.com/moj-analytical-services/splink/blob/master/splink/linker.py) feed directly into the [Linker API docs](./docs/linker.md).

There are some user restrictions on both the splink and splink_demos repos, so to make changes to either repo you will need to create a [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) and then create a [Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) which one of the Splink dev team will review.

!!! note "Shortcut"
    If you are looking through the docs and find an issue, hit the :material-file-edit: button on the top right corner of the page. This will take you to the underlying file on the Splink GitHub page.
    
    This is not supported for Tutorials or Examples as they are in a separate repo.

For small changes, such as wording and typos, changes can be made directly in GitHub. However, for larger changes it may be worth cloning the relevant repo to your local machine. This way, you can [build the docs site locally](./docs/dev_guides/changing_splink/build_docs_locally.md) to check how the changes will look in the deployed doc site.

We are trying to make our documentation as accessible to as many people as possible. If you find any problems with accessibility then please let us know by raising an issue, or feel free to put in a Pull Request with your suggested fixes.


## Contributing code
Thanks for your interest in contributing code to Splink!

There are a number of ways to get involved:

- Start work on an [existing issue](https://github.com/moj-analytical-services/splink/issues), there should be some with a [`good first issue`](https://github.com/moj-analytical-services/splink/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) flag which are a good place to start. 
- Tackle a problem you have identified. If you have identified a feature or bug, the first step is to [create a new issue](https://github.com/moj-analytical-services/splink/issues/new/choose) to explain what you have identified and what you plan to implement, then you are free to fork the repo and get coding!

In either case, we ask that you assign yourself to the relevant issue and open up [a draft PR](https://github.blog/2019-02-14-introducing-draft-pull-requests/) while you are working on your feature/bug-fix. This helps the Splink dev team keep track of developments and means we can start supporting you sooner!

!!! note "Small PRs"
    In the Splink dev team, we believe that [small Pull Requests](https://essenceofcode.com/2019/10/29/the-art-of-small-pull-requests/) make better code. They:

    - are more focused
    - increase understanding and clarity
    - are easier (and quicker) to review
    - get feedback quicker

    You can always add further PRs to build extra functionality. Starting out with a minimum viable product and iterating  makes for better software (in our opinion). It also helps get features out into the wild sooner with regular Splink releases every other Wednesday.

When making code changes, we recommend:
* [Recreating Splink's virtual environment](./docs/dev_guides/changing_splink/building_env_locally.md) to best replicate the conditions in which Splink will be used in practice. 
* [Adding tests](./docs/dev_guides/changing_splink/testing.md) to ensure your code works as expected. These will be run through GitHub Actions when a PR is opened.
* [Linting](./docs/dev_guides/changing_splink/lint_and_format.md) to ensure that code is styled consistently.

### Branching Strategy

As mentioned above, we like to keep PRs small and our philosophy on branching reflects that. Splink does not work with a `dev` branch, so all branches/forks should be made from `master` in the first instance. 

For small PRs, simply branching from `master` and merging from there is perfectly fine. However, if you have a larger feature to add we tend to try and break these up into chunks. If you have a larger feature, please consider creating a simple minimum-viable feeature and submit for review. Once this has been reviewed by the Splink dev team there are two options to consider:

1. Merge minimal feature into master then create a new branch with additional features.
2. Do not merge the initial feature branch and create additional feature branches from the reviewed branch.

The best solution often depends on the specific feature being created and any other development work happening in that area of the codebase. If you are unsure, please ask the dev team for advice on how to best structure your changes in your initial PR and we can come to a decision together.