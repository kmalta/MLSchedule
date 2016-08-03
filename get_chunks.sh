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
touch file_len
for idx in `cat $5/data_loc/chunks-$4`; do
    echo Fetching and moving: $3-chunk-$idx
    s3cmd -c $2 get s3://$3-chunk-$idx
    cat *chunk-* | tee -a train_file.$4 1> /dev/null
    wc -l *-chunk-$idx | tr -s ' ' | cut -d ' ' -f 1 >> file_len
    rm *-chunk-$idx
done

lines=0
for line in `cat $5/data_loc/file_len`; do
    lines=$(($lines + $line))
done

sed "s/num_train_this_partition:/num_train_this_partition: $lines/g" train_file.$4.meta > tmp
cat tmp > train_file.$4.meta
rm tmp
