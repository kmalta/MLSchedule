#!bin/bash

. /home/ubuntu/.profile

sudo mv /home/ubuntu/all_hosts_file /etc/hosts
sudo chown root:root /etc/hosts
sudo chmod 644 /etc/hosts

sudo mv /home/ubuntu/scripts/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh
sudo mv /home/ubuntu/hadoop_conf_files/core-site.xml $HADOOP_CONF_DIR/core-site.xml
sudo mv /home/ubuntu/hadoop_conf_files/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml
sudo mv /home/ubuntu/hadoop_conf_files/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
sudo mv /home/ubuntu/hadoop_conf_files/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
