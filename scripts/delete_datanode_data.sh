#!/bin/bash

for ip in `cat /home/ubuntu/hostfile`; do
    ssh ubuntu@$ip 'sudo rm -rf /tmp/*; sudo rm -rf /usr/local/hadoop/hadoop_data/hdfs/datanode/*'
done
