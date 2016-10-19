#!/bin/bash
ip=$(cat host_master)
ssh -i euca00/euca00-key.pem ubuntu@$ip