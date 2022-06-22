# Fast, accurate and scalable probabilistic data linkage using your choice of SQL backend.

![image](https://user-images.githubusercontent.com/7570107/85285114-3969ac00-b488-11ea-88ff-5fca1b34af1f.png)

`splink` is a Python package for probabilistic record linkage (entity resolution), within the Fellegi-Sunter framework.

It's key features are:

- It is extremely fast. It is capable of linking a million records on a laptop in around a minute.

- It is highly accurate, with support for term frequency adjustments, and sophisticated fuzzy matching logic.

- It supports multiple SQL backends, meaning it's capable of running at any scale. For smaller linkages of up to a few million records, no additional infrastructure is needed. For larger linkages, Splink currently supports Apache Spark or AWS Athena as backends.

- It produces a wide variety of interactive outputs, helping users to understand their model and diagnose linkage problems.

## Acknowledgements

We are very grateful to [ADR UK](https://www.adruk.org/) (Administrative Data Research UK) for providing funding for this work as part of the [Data First](https://www.adruk.org/our-work/browse-all-projects/data-first-harnessing-the-potential-of-linked-administrative-data-for-the-justice-system-169/) project.

We are also very grateful to colleagues at the UK's Office for National Statistics for their expert advice and peer review of this work.
