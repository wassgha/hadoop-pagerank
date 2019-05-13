#!/bin/bash

INPUT_FOLDER="/usr/local/cs417/wikipedia"
INTERMEDIATE_FOLDER="intermediate"
OUTPUT_FOLDER="topicrank-output"
ITERATIONS=10

hadoop com.sun.tools.javac.Main TopicRank.java
rm -rf $OUTPUT_FOLDER
rm -rf ${INTERMEDIATE_FOLDER}_*
jar cf TopicRank.jar TopicRank*.class
hadoop jar TopicRank.jar TopicRank $ITERATIONS $INPUT_FOLDER $INTERMEDIATE_FOLDER $OUTPUT_FOLDER
