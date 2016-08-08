#!/bin/bash
#1: remote_path
#2: path_name
path_to_host=$1/new_hostfile
path_to_pem=$1/$2

for ip in `cat $path_to_host`; do
    cat ~/.ssh/id_rsa.pub | ssh -o stricthostkeychecking=no -i $path_to_pem ubuntu@"$ip" 'cat >> .ssh/authorized_keys'
done
