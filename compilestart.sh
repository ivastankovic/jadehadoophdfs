#!/bin/bash

#Specify paths to jade home directory, the path to where the *.java classes are located
#Additionaly, specify the home directory of hadoop, and the location of it's core-site.xml file
JADE_HOME=/usr/local/jade/JADE_SRC
CODE_DIR=$JADE_HOME/code/jadehadoophdfs/
HADOOP_HOME=/usr/local/hadoop
CORE_SITE_XML=/usr/local/hadoop/etc/hadoop/core-site.xml

JADE_JAR=$JADE_HOME/lib/jade.jar

HADOOP_JAR=$HADOOP_HOME/share/hadoop/common/*
HDFS_JAR=$HADOOP_HOME/share/hadoop/hdfs/*
HADOOP_COMMON_LIB_JARS=$HADOOP_HOME/share/hadoop/common/lib/*
HDFS_LIB_JARS=$HADOOP_HOME/share/hadoop/hdfs/lib/*
YARN_LIB_JARS=$HADOOP_HOME/share/hadoop/yarn/lib/*
MR_LIB_JARS=$HADOOP_HOME/share/hadoop/mapreduce/lib/*


cd $JADE_HOME
javac -classpath $JADE_JAR:$HADOOP_JAR:$HADOOP_COMMON_LIB_JARS:$HDFS_LIB_JARS:$YARN_LIB_JARS:$MR_LIB_JARS:$HDFS_JAR -d classes $CODE_DIR/*.java 
java -cp $JADE_JAR:$HADOOP_JAR:$HADOOP_COMMON_LIB_JARS:$HDFS_LIB_JARS:$YARN_LIB_JARS:$MR_LIB_JARS:$HDFS_JAR:classes jade.Boot -gui -agents "hdfs:hadoop.agents.HDFSWriterAgent($CORE_SITE_XML)"

