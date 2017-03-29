#!/bin/bash
ip=$(cat temp_files/host_master)
ssh -i cloud_configs/aristotle/aristotle-key.pem ubuntu@$ip
