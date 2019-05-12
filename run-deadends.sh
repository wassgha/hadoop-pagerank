#!/bin/bash

INPUT_FOLDER="/usr/local/cs417/wikipedia"
OUTPUT_FOLDER="deadends-output"

hadoop com.sun.tools.javac.Main DeadEnds.java
rm -rf $OUTPUT_FOLDER
jar cf DeadEnds.jar DeadEnds*.class
hadoop jar DeadEnds.jar DeadEnds $INPUT_FOLDER $OUTPUT_FOLDER
