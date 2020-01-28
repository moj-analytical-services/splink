[![Coverage Status](https://coveralls.io/repos/github/moj-analytical-services/sparklink/badge.svg?branch=dev)](https://coveralls.io/github/moj-analytical-services/sparklink?branch=dev)
![issues-status](https://img.shields.io/github/issues-raw/moj-analytical-services/sparklink)
![python-version-dependency](https://img.shields.io/badge/python-%3E%3D3.6-blue)


# sparklink: Probabalistic record linkage at scale

WARNING:  Sparklink is work in progress and is currently in alpha testing.  Please feel free to try it, but note this software is not fully tested, and the interface is likely to change rapidly.

`sparklink` implements Fellegi-Sunter's canonical model of record linkage in Apache Spark, including EM algorithm to estimate parameters of the model.

It has been shown to be capable of de-duplicating or linking recordsets of at least 10 million records in less than an hour, on a cluster size of 10.

## Interactive demo

You can run this end-to-end demo in an interactive Jupyter notebook by clicking the button below:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/sparklink/dev)

## Documentation

Better docs to come. The best documentation is currently the demo `ipynb`, which you can find [here](https://github.com/moj-analytical-services/sparklink/blob/dev/simple_demo.ipynb).

