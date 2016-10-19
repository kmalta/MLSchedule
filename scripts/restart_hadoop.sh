#!/bin/bash

source /home/ubuntu/scripts/stop_hadoop.sh
source /home/ubuntu/scripts/delete_datanode_data.sh
sudo rm -rf /tmp/*; sudo rm -rf /usr/local/hadoop/hadoop_data/hdfs/namenode/*
source /home/ubuntu/scripts/start_hadoop.sh
