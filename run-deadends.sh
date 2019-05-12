#!/bin/bash

CLASS_NAME="DeadEnds"
INPUT_FOLDER="/usr/local/cs417/wikipedia"
OUTPUT_FOLDER="deadends-output"

hadoop com.sun.tools.javac.Main $CLASS_NAME.java
rm -rf $OUTPUT_FOLDER
jar cf $CLASS_NAME.jar $CLASS_NAME*.class
hadoop jar $CLASS_NAME.jar $CLASS_NAME $INPUT_FOLDER $OUTPUT_FOLDER
