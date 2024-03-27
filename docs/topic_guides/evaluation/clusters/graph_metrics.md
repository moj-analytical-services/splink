# Graph metrics

Graph metrics quantify the characteristics of a graph. A simple example of a graph metric is [cluster size](), which is the number of nodes within a cluster.

For data linking with Splink, it is useful to sort graph metrics into three categories:

* [Node metrics]()
* [Edge metrics]()
* [Cluster metrics]()

Each of these are defined below together with examples and explanations of how they can be applied to linked data to evaluate cluster quality. The examples given are of all metrics currently available in Splink.

!!! note

    It is important to bear in mind that whilst graph metrics can be very useful for assessing linkage quality, they are rarely definitive, especially when taken in isolation. A more comprehensive picture can be built by considering various metrics in conjunction with one another.

    It is also important to consider metrics within the context of their distribution and the underlying dataset. For example: a cluster density (see below) of 0.4 might seem low but could actually be above average for the dataset in question; a cluster of size 80 might be suspiciously large for one dataset but not for another.


## ⚫️ Node metrics

Node metrics quantify the properties of the nodes which live within clusters.

### Example: node degree

Node degree is the number of edges connected to a node.

High node degree is generally considered good as it means there are many edges in support of records in a cluster being linked. Nodes with low node degree could indicate links being missed (false negatives).

However, erroneous links (false positives) could also be the reason for high node degree, so it can be useful to validate the edges of highly connected nodes.

It is important to consider [cluster size]() when looking at node degree. By definition, larger clusters contain more nodes to form links between, allowing nodes within them to attain higher degrees compared to those in smaller clusters. Consequently, low node degree within larger clusters can carry greater significance.

Bear in mind, that the degree of a single node in a cluster isn't necessarily representative of the overall connectedness of a cluster. This is where [cluster centralisation]() can help.

## 🔗 Edge metrics

Edge metrics quantify the properties of the edges within a cluster. 

### Example: 'is bridge'

An edge is classified as a 'bridge' if its removal splits a cluster into two smaller clusters.

[insert picture]

Bridges can be signalers of false positives in linked data, especially when joining two highly connected sub-clusters. Examining bridges can shed light on potential errors in the linking process leading to the formation of false positive links.

## :fontawesome-solid-circle-nodes: Cluster metrics

Cluster metrics refer to the characteristics of a cluster as a whole, rather than the individual nodes and edges it contains.

### Example: cluster size

Cluster size refers to the number of nodes within a cluster.

When thinking about cluster size, it is important to consider the size of the biggest clusters produced and ask yourself, does this seem reasonable for the dataset being linked? For example, does it make sense that one person is appearing hundreds of times in the linked data resulting in a cluster of over 100 nodes? If the answer is no, then false positives links are probably being formed. This could be, for example, due to having blocking rules which are too loose.

If you don't have an intuition of what seems reasonable, then it is worth inspecting a sample of the largest clusters in Splink's [Cluster Studio Dashboard]() to validate (or invalidate) links. From there you can develop an understanding of what maximum cluster size to expect.

There also might be a lower bound on cluster size. For example, when linking two datasets in which you know people appear least once in each, the minimum expected size of cluster will be 2. Clusters smaller than the minimum size indicate links have been missed. This could be due to blocking rules not letting through all record comparisons of true matches.

### Example: cluster density

The density of a cluster is given by the number of edges it contains divided by the maximum possible number of edges. Density ranges from 0 to 1. A density of 1 means that all nodes are connected to all other nodes in a cluster.

[picture: edges vs max possible edges]

When evaluating clusters, a high density (closer to 1) is generally considered good as it means there are many edges in support of the records in a cluster being linked.

A low density could indicate links being missed. This could happen, for example, if blocking rules are too tight or the clustering threshold is too high.
A sample of low density clusters can be inspected in Splink [Cluster Studio Dashboard]() via the option `sampling_method = "lowest_density_clusters_by_size"`. When inspecting a cluster, ask yourself the question: why aren't more links being formed between record nodes?

Bear in mind, small clusters are more likely to achieve a higher density as fewer record comparisons are required to form the maximum edges possible (the maximum density of 1 for a cluster of size 3 can be achieved with only 3 pairwise record comparisons).

Therefore it's important to consider a range of sizes when evaluating density to ensure you're not just focussed on very big clusters. Smaller clusters also have the advantage of being easier to assess by eye. This is why the option `sampling_method = "lowest_density_clusters_by_size"` performs stratified sampling across different cluster sizes.

<!-- With each increase in N, the number of possible edges increases. It might be 'harder' for bigger clusters to attain a higher density because blocking rules may prevent all record comparisons of nodes within a cluster. -->

### Example: cluster centralisation

[Cluster centralisation]("https://en.wikipedia.org/wiki/Centrality#Degree_centrality") is defined as the deviation from maximum [node degree]() normalised with respect to the maximum possible value. In other words, cluster centralisation tells us about the concentration of edges in a cluster. Centralisation ranges from 0 to 1.

A high cluster centralisation (closer to 1) indicates that a few nodes are home to significantly more connections compared to the rest of the nodes in a cluster. This can help identify clusters containing nodes with a lower number of connections (low node degree) relative to what is possible for that cluster. 

Low centralisation suggests that edges are more evenly distributed amongst nodes in a cluster. Low centralisation can be good if all nodes within a clusters enjoy many connections. However low centralisation could also indicate that all nodes are not as highly connected as they could be. To check for this, you can look at low centralisation in conjunction with low [node degree]() or [low density]().

[maybe include a picture to help aid understanding]

<br>

A guide on [how to compute all the graph metrics mentioned above with Splink]() is given in the next chapter.
