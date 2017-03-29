#!/bin/bash
grep MemTotal /proc/meminfo | awk '{print $2}' | xargs -I {} echo "scale=4; {}/1024" | bc > mem_file
