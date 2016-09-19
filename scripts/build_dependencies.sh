#!/bin/bash
tar -xf scripts.tar.gz

sudo apt-get update
sudo apt-get -y install g++ make autoconf git libtool uuid-dev openssh-server cmake bc libopenmpi-dev openmpi-bin libssl-dev libnuma-dev 
sudo apt-get -y install python-dev python-numpy python-pip python-scipy python-yaml protobuf-compiler subversion libxml2-dev libxslt-dev 
sudo apt-get -y install ssh rsync zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev openjdk-7-jdk


wget http://www.scala-lang.org/files/archive/scala-2.11.7.tgz
sudo mkdir /home/ubuntu/scala
sudo tar xvf scala-2.11.7.tgz -C /home/ubuntu/scala/

echo -e "export SCALA_HOME=/home/ubuntu/scala-2.11.7" >> /home/ubuntu/.bashrc
echo -e "export PATH=$SCALA_HOME/bin:$PATH" >> /home/ubuntu/.bashrc
source /home/ubuntu/.bashrc

cd /home/ubuntu
wget http://apache.claz.org/hadoop/common/hadoop-2.7.3/hadoop-2.7.3.tar.gz
tar -xvvf hadoop-2.7.3.tar.gz


cd /home/ubuntu
wget http://apache.mirrors.ionfish.org/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
tar -xvvf spark-2.0.0-bin-hadoop2.7.tgz
mv spark-2.0.0-bin-hadoop2.7 spark-2.0.0

echo "export PATH='home/ubuntu/spark-2.0.0/bin/:$PATH'" >> ~/.bashrc
. ~/.bashrc


sudo mv /home/ubuntu/hadoop-2.7.3 /usr/local/hadoop
echo -e "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64" >> /home/ubuntu/.profile
echo -e "export HADOOP_HOME=/usr/local/hadoop" >> /home/ubuntu/.profile
echo -e "export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop" >> /home/ubuntu/.profile
. /home/ubuntu/.profile

echo -e "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin" >> /home/ubuntu/.profile
echo -e "export HADOOP_MAPRED_HOME=$HADOOP_HOME" >> /home/ubuntu/.profile
echo -e "export HADOOP_COMMON_HOME=$HADOOP_HOME" >> /home/ubuntu/.profile
echo -e "export HADOOP_HDFS_HOME=$HADOOP_HOME" >> /home/ubuntu/.profile
echo -e "export YARN_HOME=$HADOOP_HOME" >> /home/ubuntu/.profile
echo -e "export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native" >> /home/ubuntu/.profile
echo -e "export HADOOP_OPTS=-Djava.library.path="$HADOOP_HOME"/lib/native" >> /home/ubuntu/.profile
. /home/ubuntu/.profile


mkdir data; mkdir jars; mkdir model

sudo mv /home/ubuntu/ssh_config /etc/ssh/ssh_config
sudo chown root:root /etc/ssh/ssh_config
sudo chmod 644 /etc/ssh/ssh_config


cd /home/ubuntu
git clone https://github.com/eucalyptus/s3cmd
cd /home/ubuntu/s3cmd
sudo pip install setuptools
sudo python setup.py install

rm -rf *gz

cd /home/ubuntu
source /home/ubuntu/scripts/build_image_script.sh
