#!/bin/bash
# 1: Master IP
tar -cf scripts_for_master.tar.gz kmalta.pem add_public_key_script.sh build_image_script.sh create_ssh_keygen.sh
scp -o stricthostkeychecking=no -i kmalta.pem scripts_for_master.tar.gz ubuntu@$1:~/
scp -o stricthostkeychecking=no -i kmalta.pem build_master_dependencies.sh ubuntu@$1:~/
ssh -o stricthostkeychecking=no -i kmalta.pem -i kmalta.pem ubuntu@$1 'source build_master_dependencies.sh'
