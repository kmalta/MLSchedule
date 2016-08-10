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
rm file_len
touch file_len
rm ~/s3error
touch ~/s3error
for idx in `cat $1/chunks-$4`; do
    s3cmd -c $2 get s3://$3-chunk-$idx >> ~/s3error
    cat *chunk-* | tee -a train_file.$4 1> /dev/null
    wc -l *-chunk-$idx | tr -s ' ' | cut -d ' ' -f 1 >> file_len
    rm *-chunk-$idx
done

lines=0
for line in `cat $1/file_len`; do
    lines=$(($lines + $line))
done

sed "s/num_train_this_partition:.*/num_train_this_partition: $lines/g" train_file.$4.meta > tmp
cat tmp > train_file.$4.meta
rm tmp
