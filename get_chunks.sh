#!/bin/bash
#1: Remote Path
#2: Bucket-path-name
#3: ip_idx
cd $1/data_loc
for idx in `cat chunks-$3`; do
    sudo s3cmd -c ../*-s3cfg get s3://$2-chunk-$idx
done
sudo cat *chunk* >> train_file.$3
