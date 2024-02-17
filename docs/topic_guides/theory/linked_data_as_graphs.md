# Linked data as graphs

A graph is a collection of points (often referred to as nodes or vertices) connected by lines (often referred to as edges).

![Image placeholder](../../img/clusters/graph.png){:width="50%"}

(Then...)
<br>
Graphs provide a natural way to represent linked data...OR...
<br>
Graphs provide a natural way to represent the data linking process...OR...
<br>
another thing altogether

Within the context of data linking, the nodes of a graph represent records and the edges represent the links between them. We refer to groups of interconnected nodes (records) as **clusters**.

![Image placeholder](../../img/clusters/graph.png){:width="50%"}

For example, if linking people together, a cluster would look something like this

![Image placeholder](../../img/clusters/graph.png){:width="50%"}

!!! note

    For clusters produced with Splink, every edge comes with an associated Splink score (the probability of two records being a match). The clustering threshold (`match_probability_threshold`) supplied by the user determines which records are included in a cluster, as any links (edges) between records with a match probability below this threshold are excluded.

    Clusters, specifically cluster IDs, are the ultimate output of a Splink model.