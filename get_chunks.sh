#!/bin/bash
#1: Data Path
#2: Config Path
#3: Bucket-path-name
#4: ip_idx
#5: remote path

# mkdir $5/data_loc
cd $1
touch train_file.$4
rm train_file.$4
touch train_file.$4
touch file_len
for idx in `cat $1/chunks-$4`; do
    s3cmd -c $2 get s3://$3-chunk-$idx
    cat *chunk-* | tee -a train_file.$4 1> /dev/null
    rm *-chunk-$idx
done

lines=$(wc -l train_file.$4 | tr -s ' ' | cut -d ' ' -f 1)

sed "s/num_train_this_partition:.*/num_train_this_partition: $lines/g" train_file.$4.meta > tmp
cat tmp > train_file.$4.meta
rm tmp
