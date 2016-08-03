#!/bin/bash
#1: Data Path
#2: Config Path
#3: Bucket-path-name
#4: ip_idx
#5: remote path
#sudo mkdir $1
mkdir $5/data_loc
cd $5/data_loc
#sudo touch $1/train_file.$4
for idx in `cat $5/data_loc/chunks-$4`; do
    echo Fetching and moving: $3-chunk-$idx
    s3cmd -c $2 get s3://$3-chunk-$idx
    cat *chunk-* | tee -a train_file.$4 1&> /dev/null
    rm *-chunk-$idx
    sudo rm 1
done
