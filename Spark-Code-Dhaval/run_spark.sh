#! /bin/bash

# RUN THE FOLLOWING COMMAND IN THE SHELL BEFORE RUNNING THIS FILE
# $ sbt clean assembly

# ALL PATHS MUST BE ABSOLUTE PATHS IF THE PROGRAM IS RUN USING NOHUP

# SET SPARK HOME IF NOT SET
SPARK_HOME='/usr/local/spark'

# Basic spark-submit command for the cluster
# CHANGE SPARK MASTER
submit_command="$SPARK_HOME/bin/spark-submit   --master spark://dhaval01:7077"

# uncomment the following line for local cluster
#submit_command="$SPARK_HOME/bin/spark-submit --master local"

# FIND target jar files created by sbt
jar_files=$(find /home/hadoop/final/final/target -iname "*assembly*.jar")

# The following durations are in seconds
batchSize=1
windowTime=43200
slidingWindowDuration=3600

# 1 hour = 3600 seconds
# 12 hour = 43200 seconds

# the following options are not used now. but we are keeping them in case we want to limit the outputs
nTags=200
nTweets=2
nPlaces=200

#set the search string here
searchString="donald trump"

# Create the final command
cmd="$submit_command $jar_files $batchSize $windowTime $slidingWindowDuration $nTags $nTweets $nPlaces $searchString"

# Execute the command
# Set output files below
# error.log contains stderr output
# count_and_sentiments.txt contains stdout output
$cmd 2> info.log  >> count_and_sentiment.txt
