[![Coverage Status](https://coveralls.io/repos/github/moj-analytical-services/sparklink/badge.svg?branch=dev)](https://coveralls.io/github/moj-analytical-services/sparklink?branch=dev)
![issues-status](https://img.shields.io/github/issues-raw/moj-analytical-services/sparklink)
![python-version-dependency](https://img.shields.io/badge/python-%3E%3D3.6-blue)


# sparklink: Probabalistic record linkage at scale

WARNING:  Sparklink is work in progress and is currently in alpha testing.  Please feel free to try it, but note this software is not fully tested, and the interface is likely to change rapidly.

`sparklink` implements Fellegi-Sunter's canonical model of record linkage in Apache Spark, including EM algorithm to estimate parameters of the model.

The aim of `sparklink` is to:

- Work at much greater scale than current open source implementations (100 million records +).

- Get results faster than current open source implementations - with runtimes of less than an hour.

- Have a highly transparent methodology, so the match scores can be easily explained both graphically and in words

- Have accuracy similar to some of the best alternatives

## Interactive demo

You can run this end-to-end demo in an interactive Jupyter notebook by clicking the button below:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/sparklink/dev?urlpath=lab/tree/demo_notebooks/quickstart_demo.ipynb)

## Documentation

Better docs to come. The best documentation is currently the demo `ipynbs`, which you can find [here](https://github.com/moj-analytical-services/sparklink/tree/dev/demo_notebooks).

