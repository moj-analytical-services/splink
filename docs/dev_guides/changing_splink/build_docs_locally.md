## Building docs locally

Before building the docs locally, you will need to follow the [development quickstart](./development_quickstart.md)
to set up the necessary environment.
You cannot skip this step, because some Splink docs Markdown is auto-generated using the Splink development environment.

Once you've done that,
to rapidly build the documentation and immediately see changes you've made you can use [this script](https://github.com/moj-analytical-services/splink/blob/master/scripts/make_docs_locally.sh)
**outside your Splink development environment**:

```sh
source scripts/make_docs_locally.sh
```

This is much faster than waiting for github actions to run if you're trying to make fiddly changes to formatting, etc.
