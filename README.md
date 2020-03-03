[![Coverage Status](https://coveralls.io/repos/github/moj-analytical-services/splink/badge.svg?branch=dev)](https://coveralls.io/github/moj-analytical-services/splink?branch=dev)
![issues-status](https://img.shields.io/github/issues-raw/moj-analytical-services/splink)
![python-version-dependency](https://img.shields.io/badge/python-%3E%3D3.6-blue)


# splink: Probabalistic record linkage at scale

WARNING:  Splink is work in progress and is currently in alpha testing.  Please feel free to try it, but note this software is not fully tested, and the interface is likely to change rapidly.

`splink` implements Fellegi-Sunter's canonical model of record linkage in Apache Spark, including EM algorithm to estimate parameters of the model.

The aim of `splink` is to:

- Work at much greater scale than current open source implementations (100 million records +).

- Get results faster than current open source implementations - with runtimes of less than an hour.

- Have a highly transparent methodology, so the match scores can be easily explained both graphically and in words

- Have accuracy similar to some of the best alternatives

## Interactive demo

You can run demos of `splink` in an interactive Jupyter notebook by clicking the button below:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab/tree/index.ipynb)

## Documentation

Better docs to come. The best documentation is currently a series of demonstrations notebooks in the [splink_demos](https://github.com/moj-analytical-services/splink_demos) repo.

