#!/bin/bash

CLASS_NAME="DeadEnds"
INPUT_FOLDER="/usr/local/cs417/instacart_2017_05_01"
OUTPUT_FOLDER="output"

hadoop com.sun.tools.javac.Main $CLASS_NAME.java
rm -rf output
jar cf $CLASS_NAME.jar $CLASS_NAME*.class
hadoop jar $CLASS_NAME.jar $CLASS_NAME $INPUT_FOLDER $OUTPUT_FOLDER
