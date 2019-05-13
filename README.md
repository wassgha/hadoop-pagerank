### Wikipedia PageRank/TopicRank Calculation

This assignment aims to calculate PageRank and TopicRank for the core ["Wikipedia corpus"](https://www.instacart.com/datasets/grocery-shopping-2017) obtained through the ["Stanford Network Analysis Project"](https://snap.stanford.edu/data/wiki-topcats.html) using Hadoop and AWS Elastic MapReduce. 

#### Requirements

Running this program requires the installation of `Hadoop` and its Java dependencies.

#### Running the programs

This project is comprised of three MapReduce programs. The first (`DeadEnds`) looks through the graph and tries to find dead-end nodes (that do not have out-links), the second (`PageRank`) computes the overall PageRank for all nodes in the graph. The last (`TopicRank`), computes the TopicRank for pages in the `Electrical_engineering` topic (which can be changes by inspection of the file).

To the run the programs, we have included small utility programs named `run-deadends.sh`, `run-pagerank.sh` and `run-topicrank.sh`. The utilities cleans up previous intermediate and output folders, compile the java classes and archive them into jar files that can be exported to AWS EMR and finally run the pipeline on a hard coded input path `/usr/local/cs417/wikipedia` which can be changed manually by editing the `run-deadends.sh`, `run-pagerank.sh` or `run-topicrank.sh` files.

1.  Give the program execution privileges by using

```
chmod +x run-{deadends | pagerank | topicrank }.sh
```

2.  Make sure the `/usr/local/cs417/wikipedia` file exists on your computer or change it in `run-{deadends | pagerank | topicrank }.sh`

3.  Execute the utility using

```
./run-{deadends | pagerank | topicrank }.sh
```

#### Output

The output will be written to the `./deadends-output`, `./pagerank-output` or `./topicrank-output` folders based on the utility script you're running in the form of a single file containing the nodes, their page rank and their page name sorted by PageRank.
