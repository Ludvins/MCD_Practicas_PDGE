#!/bin/bash

file=$1
name=$2

HADOOP_CLASSPATH=$(hadoop classpath)
#echo $HADOOP_CLASSPATH

rm -rf ${file}
mkdir -p ${file}

javac -classpath $HADOOP_CLASSPATH -d ${file} ${file}.java
jar -cvf ${name}.jar -C ${file} .
