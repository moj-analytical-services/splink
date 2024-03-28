# Linked data as graphs

A graph is a collection of points (often referred to as nodes or vertices) connected by lines (often referred to as edges).

![Basic Graph](../../img/clusters/basic_graph.drawio.png){:width="80%"}

Then a group of interconnected nodes is referred to as a **cluster**.

![Basic Cluster](../../img/clusters/basic_graph_cluster.drawio.png){:width="80%"}

Graphs provide a natural way to represent linked data, where the nodes of a graph represent records being linked and the edges represent the links between them. So, if we have 5 records (A-E) in our dataset(s), with links between them, this can be viewed as a graph.

![Basic Graph - Records](../../img/clusters/basic_graph_records.drawio.png){:width="80%"}

For example, when linking people together, a cluster represents the all of the records in our dataset(s) that refer to the same person. We can give this cluster a new identifier (F) to refer to this single person.

![Basic Person Cluster](../../img/clusters/basic_graph_cluster_person.drawio.png){:width="80%"}

!!! note

    For clusters produced with Splink, every edge comes with an associated Splink score (the probability of two records being a match). The clustering threshold (`match_probability_threshold`) supplied by the user determines which records are included in a cluster, as any links (edges) between records with a match probability below this threshold are excluded.

    Clusters, specifically cluster IDs, are the ultimate output of a Splink pipeline.