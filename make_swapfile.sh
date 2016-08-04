#!/bin/bash
#1: Num Blocks
sudo dd if=/dev/zero of=/swapfile bs=1024 count=$1
sudo chmod 0600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo "/swapfile          swap            swap    defaults        0 0" | sudo tee -a /etc/fstab
