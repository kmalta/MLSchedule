#!/bin/bash
df -m | tail -n +4 | head -n +1 | awk '{print $2}' >> disk_space_file
df -m | tail -n +4 | head -n +1 | awk '{print $3}' >> disk_space_file
