#!/bin/bash
file="/home/ubuntu/time_file"
if [ -f "$file" ]
then
    echo "1" > /home/ubuntu/exists
else
    echo "0" > /home/ubuntu/exists
fi