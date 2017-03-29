#!/bin/bash
f=$1
p=$2

file_size=$(wc -l < "$f")
percent_size_as_float=$(echo "$file_size*$p" | bc)
float_to_int=$(printf %.0f "$percent_size_as_float")
grab_percent=$(head -n "$float_to_int" "$f")
new_fn=$(printf "%s_exp" "$f")
printf "$grab_percent" > $new_fn