# Fast, accurate and scalable probabilistic data linkage using your choice of SQL backend.

![image](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)

`splink` is a Python package for probabilistic record linkage (entity resolution).

Its key features are:

- It is extremely fast. It is capable of linking a million records on a laptop in around a minute.

- It is highly accurate, with support for term frequency adjustments, and sophisticated fuzzy matching logic.

- It supports running linkage against multiple SQL backends, meaning it's capable of running at any scale. For smaller linkages of up to a few million records, no additional infrastructure is needed . For larger linkages, Splink currently supports Apache Spark or AWS Athena as backends.

- It produces a wide variety of interactive outputs, helping users to understand their model and diagnose linkage problems.

The core linkage algorithm is an implementation of Fellegi-Sunter's canonical model of record linkage, with various customisations to improve accuracy. Splink includes an implementation of the Expectation Maximisation algorithm, meaning that record linkage can be performed using an unsupervised approch (i.e. labelled training data is not needed).

## Documentation

The homepage for the Splink documentation can be found [here](https://moj-analytical-services.github.io/splink/). Interactive demos can be found [here](https://github.com/moj-analytical-services/splink_demos/tree/splink3_demos), or by clicking the following Binder link:
[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/moj-analytical-services/splink_demos/splink3_demos?urlpath=lab)

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work.
