#!/bin/bash
scp -o stricthostkeychecking=no -i kmalta.pem build_worker_dependencies.sh build_image_script.sh ubuntu@$1:~/
ssh -o stricthostkeychecking=no -i kmalta.pem -i kmalta.pem ubuntu@$1 'source build_worker_dependencies.sh'
