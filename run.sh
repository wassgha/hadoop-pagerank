#!/bin/bash

if [ $# -lt 1 ]; then
  echo 1>&2 "$0: Class name missing"
  exit 2
elif [ $# -gt 1 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

hadoop com.sun.tools.javac.Main $1.java
rm -rf output && rm -rf intermediate_1 && rm -rf intermediate_2
jar cf $1.jar $1*.class
hadoop jar $1.jar $1 /usr/local/cs417/instacart_2017_05_01 intermediate output
