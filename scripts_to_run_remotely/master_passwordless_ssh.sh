#!/bin/bash
#1: remote_path
#2: path_name
#3: master_ip
#4: master_port
#5: master_host
#6: master_priv_ip

host_path=/home/ubuntu/hostfile
pem_path=/home/ubuntu/$2

source scripts/create_ssh_keygen.sh
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

sudo ufw disable
sudo setenforce 0
sudo ufw allow 4000:9000/tcp
sudo ufw allow 50000:54311/tcp

for ip in `cat $host_path`; do
    cat ~/.ssh/id_rsa.pub | ssh -q -o stricthostkeychecking=no -i $pem_path ubuntu@$ip 'cat >> ~/.ssh/authorized_keys'
done


for ip in $(echo `cat $host_path` $master_ip); do
    ssh -t -t -q -o stricthostkeychecking=no ubuntu@$ip 'source scripts/create_ssh_keygen.sh;source scripts/set_local_host.sh ' $3 $4 $5 $host_path $pem_path $6
done
