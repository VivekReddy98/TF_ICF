#!/bin/bash

rm -rf output*/
rm -rf *.class
javac TFICF.java
jar cf TFICF.jar TFICF*.class
hadoop jar TFICF.jar TFICF input0 input1 &> hadoop_output.txt
hdfs dfs -get /user/vkarri/output1 .
hdfs dfs -get /user/vkarri/output0 .


