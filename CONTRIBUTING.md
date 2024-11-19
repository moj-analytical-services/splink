# Contributing to Splink

Contributing to an open source project takes many forms. Below are some of the ways you can contribute to Splink!

## Asking questions

If you have a question about Splink, we recommended asking on our GitHub [discussion board](https://github.com/moj-analytical-services/splink/discussions). This means that other users can benefit from the answers too! On that note, it is always worth checking if a similar question has been asked (and answered) before.

## Reporting issues

Is something broken? Or not acting how you would expect? Are we missing a feature that would make your life easier? We want to know about it!

When reporting [issues](https://github.com/moj-analytical-services/splink/issues) please include as much detail as possible about your operating system, Splink version, python version and which SQL backend you are using. Whenever possible, please also include a brief, self-contained code example that demonstrates the problem. It is particularly helpful if you can look through the existing issues and provide links to any related issues.

## Contributing to documentation

Contributions to Splink are not limited to the code. Feedback and input on our documentation from a user's perspective is extremely valuable - even something as small as fixing a typo. More generally, if you are interested in starting to work on Splink, documentation is a great way to get those first commits!

The easiest way to contribute to the documentation is by clicking the pencil icon at the top right of the docs page you want to edit.
This will automatically create a fork of the Splink repository on GitHub and make it easy to open a pull request with your changes,
which one of the Splink dev team will review.

If you need to make a larger change to the docs, this workflow might not be the best, since you won't get to see the effects
of your changes before submitting them.
To do this, you will need to create a [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) of the Splink repo,
then clone your fork to your computer.
Then, you can edit the documentation in the [docs folder](https://github.com/moj-analytical-services/splink/tree/master/docs)
(and API documentation, which can be found as docstrings in the code itself) locally.
To see what the docs will look like with your changes, you can
[build the docs site locally](https://moj-analytical-services.github.io/splink/dev_guides/changing_splink/build_docs_locally.html).
When you are happy with your changes, commit and push them to your fork, then
create a [Pull Request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

We are trying to make our documentation as accessible to as many people as possible. If you find any problems with accessibility then please let us know by raising an issue, or feel free to put in a Pull Request with your suggested fixes.

## Contributing code

Thanks for your interest in contributing code to Splink!

There are a number of ways to get involved:

- Start work on an [existing issue](https://github.com/moj-analytical-services/splink/issues), there should be some with a [`good first issue`](https://github.com/moj-analytical-services/splink/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) flag which are a good place to start.
- Tackle a problem you have identified. If you have identified a feature or bug, the first step is to [create a new issue](https://github.com/moj-analytical-services/splink/issues/new/choose) to explain what you have identified and what you plan to implement, then you are free to fork the repository and get coding!

In either case, we ask that you assign yourself to the relevant issue and open up [a draft pull request (PR)](https://github.blog/2019-02-14-introducing-draft-pull-requests/) while you are working on your feature/bug-fix. This helps the Splink dev team keep track of developments and means we can start supporting you sooner!

You can always add further PRs to build extra functionality. Starting out with a minimum viable product and iterating makes for better software (in our opinion). It also helps get features out into the wild sooner.

To get set up for development locally, see the [development quickstart](https://moj-analytical-services.github.io/splink/dev_guides/changing_splink/development_quickstart.html).

## Best practices

When making code changes, we recommend:

- [Adding tests](https://moj-analytical-services.github.io/splink/dev_guides/changing_splink/testing.html) to ensure your code works as expected. These will be run through GitHub Actions when a PR is opened.
- [Linting](https://moj-analytical-services.github.io/splink/dev_guides/changing_splink/lint_and_format.html) to ensure that code is styled consistently.

### Branching Strategy

All pull requests (PRs) should target the `master` branch.

We believe that [small Pull Requests](https://essenceofcode.com/2019/10/29/the-art-of-small-pull-requests/) make better code. They:

- are more focused
- increase understanding and clarity
- are easier (and quicker) to review
- get feedback quicker

If you have a larger feature, please consider creating a simple minimum-viable feature and submit for review. Once this has been reviewed by the Splink dev team there are two options to consider:

1. Merge minimal feature, then create a new branch with additional features.
2. Do not merge the initial feature branch, create additional feature branches from the reviewed branch.

The best solution often depends on the specific feature being created and any other development work happening in that area of the codebase. If you are unsure, please ask the dev team for advice on how to best structure your changes in your initial PR and we can come to a decision together.
