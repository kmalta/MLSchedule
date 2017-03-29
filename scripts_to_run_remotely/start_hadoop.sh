#!bin/bash
#1: num_times
. /home/ubuntu/.profile
echo -e "Y" | hdfs namenode -format
start-dfs.sh
start-yarn.sh
