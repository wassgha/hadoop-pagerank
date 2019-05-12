#!/bin/bash

# INPUT_FOLDER="/usr/local/cs417/wikipedia"
INPUT_FOLDER="input"
INTERMEDIATE_FOLDER="intermediate"
OUTPUT_FOLDER="pagerank-output-with-taxation"
ITERATIONS=10

hadoop com.sun.tools.javac.Main PageRank.java
rm -rf $OUTPUT_FOLDER
rm -rf ${INTERMEDIATE_FOLDER}_*
jar cf PageRank.jar PageRank*.class
hadoop jar PageRank.jar PageRank $ITERATIONS $INPUT_FOLDER $INTERMEDIATE_FOLDER $OUTPUT_FOLDER
