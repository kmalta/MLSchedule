#!/bin/bash
path_to_host='/home/ubuntu/petuum/new_hostfile'
path_to_pem='/home/ubuntu/petuum/kmalta.pem'

for ip in `cat $path_to_host`; do
    cat ~/.ssh/id_rsa.pub | ssh -i kmalta.pem -o stricthostkeychecking=no -i $path_to_pem ubuntu@"$ip" 'cat >> .ssh/authorized_keys'
done
