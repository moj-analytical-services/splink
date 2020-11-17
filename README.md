![image](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)

[![Coverage Status](https://coveralls.io/repos/github/moj-analytical-services/splink/badge.svg?branch=master)](https://coveralls.io/github/moj-analytical-services/splink?branch=master)
![issues-status](https://img.shields.io/github/issues-raw/moj-analytical-services/splink)
![python-version-dependency](https://img.shields.io/badge/python-%3E%3D3.6-blue)


# splink: Probabilistic record linkage and deduplication at scale

`splink` implements Fellegi-Sunter's canonical model of record linkage in Apache Spark, including EM algorithm to estimate parameters of the model.

The aims of `splink` are to:

- Work at much greater scale than current open source implementations (100 million records +).

- Get results faster than current open source implementations - with runtimes of less than an hour.

- Have a highly transparent methodology, so the match scores can be easily explained both graphically and in words

- Have accuracy similar to some of the best alternatives

## Installation

`splink` is a Python package.  It uses the Spark Python API to execute data linking jobs in a Spark cluster.  It has been tested in Apache Spark 2.3 and 2.4.

Install splink using

`pip install splink`

## Interactive demo

You can run demos of `splink` in an interactive Jupyter notebook by clicking the button below:

[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/master?urlpath=lab/tree/index.ipynb)

## Documentation

The best documentation is currently a series of demonstrations notebooks in the [splink_demos](https://github.com/moj-analytical-services/splink_demos) repo.

We also provide an interactive `splink` settings editor and example settings [here](https://moj-analytical-services.github.io/splink_settings_editor/).  A tool to generate custom `m` and `u` probabilities can be found [here](https://observablehq.com/@robinl/m-and-u-probabilities).

The statistical model behind `splink` is the same as that used in the R [fastLink package](https://github.com/kosukeimai/fastLink).  Accompanying the fastLink package is an [academic paper](http://imai.fas.harvard.edu/research/files/linkage.pdf) that describes this model.  This is the best place to start for users wanting to understand the theory about how `splink` works.

You can read a short blog post about `splink` [here](https://robinlinacre.com/introducing_splink/).

## Videos

You can find a short video introducing `splink` and running though an introductory demo [here](https://www.youtube.com/watch?v=_8lV2Lbd6Xs&feature=youtu.be&t=1295).

A 'best practices and performance tuning' tutorial can be found [here](https://www.youtube.com/watch?v=HzcqrRvXhCE).

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work.
