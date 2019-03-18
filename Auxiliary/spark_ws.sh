#!/bin/bash

# WCC
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/cit-Patents.txt hdfs:///user/shriramr/output0.txt 8 > log0.txt &
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 4 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/soc-livj.txt hdfs:///user/shriramr/output1.txt 32 > log1.txt &
spark-submit --class in.ds256.Assignment1.Spark.wcc --master yarn --deploy-mode cluster --num-executors 7 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/orkut.txt hdfs:///user/shriramr/output2.txt 56 > log2.txt &

# Conductance
spark-submit --class in.ds256.Assignment1.Spark.conductance --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/cit-Patents.txt hdfs:///user/shriramr/output0.txt 8 > log0.txt &
spark-submit --class in.ds256.Assignment1.Spark.conductance --master yarn --deploy-mode cluster --num-executors 4 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/soc-livj.txt hdfs:///user/shriramr/output1.txt 32 > log1.txt &
spark-submit --class in.ds256.Assignment1.Spark.conductance --master yarn --deploy-mode cluster --num-executors 7 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/orkut.txt hdfs:///user/shriramr/output2.txt 56 > log2.txt &

# PR
spark-submit --class in.ds256.Assignment1.Spark.pr --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/cit-Patents.txt hdfs:///user/shriramr/output0.txt 8 > log0.txt &
spark-submit --class in.ds256.Assignment1.Spark.pr --master yarn --deploy-mode cluster --num-executors 4 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/soc-livj.txt hdfs:///user/shriramr/output1.txt 32 > log1.txt &
spark-submit --class in.ds256.Assignment1.Spark.pr --master yarn --deploy-mode cluster --num-executors 7 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/orkut.txt hdfs:///user/shriramr/output2.txt 56 > log2.txt &

# Span
spark-submit --class in.ds256.Assignment1.Spark.span --master yarn --deploy-mode cluster --num-executors 1 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/cit-Patents.txt hdfs:///user/shriramr/output0.txt 8 > log0.txt &
spark-submit --class in.ds256.Assignment1.Spark.span --master yarn --deploy-mode cluster --num-executors 4 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/soc-livj.txt hdfs:///user/shriramr/output1.txt 32 > log1.txt &
spark-submit --class in.ds256.Assignment1.Spark.span --master yarn --deploy-mode cluster --num-executors 7 --driver-memory 512m --executor-memory 8G --executor-cores 4  Code/target/Assignment1-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs:///user/ds256/a1/orkut.txt hdfs:///user/shriramr/output2.txt 56 > log2.txt &
