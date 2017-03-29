#!bin/bash
ip=`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1' | head -n 1`

address='euca-'${ip//./-}

. /home/ubuntu/.profile
sudo mkdir -p $HADOOP_HOME/hadoop_data/hdfs/namenode
sudo mv /home/ubuntu/masters $HADOOP_CONF_DIR/masters
sudo mv /home/ubuntu/slaves $HADOOP_CONF_DIR/slaves
sudo chown -R ubuntu $HADOOP_HOME


cp /home/ubuntu/spark-2.0.0/conf/spark-env.sh.template /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e "export SPARK_MASTER_IP="$ip >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e "export SPARK_MASTER_HOST="$address >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e 'export SPARK_LOCAL_IP='$ip >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e 'export SPARK_LOCAL_HOST='$address >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e 'export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop' >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
echo -e 'export SPARK_LOCAL_DIRS=/mnt/spark' >> /home/ubuntu/spark-2.0.0/conf/spark-env.sh
