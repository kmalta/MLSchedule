#!/bin/bash
sudo dd if=/dev/zero of=/swapfile bs=4096 count=65
sudo chmod 0600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo "/swapfile          swap            swap    defaults        0 0" | sudo tee -a /etc/fstab