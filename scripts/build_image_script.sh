#!/bin/bash
sudo rm -f ~/.bash_history
sudo rm -f /etc/udev/rules.d/70*-net.rules
sudo rm -rf /root/linux-rootfs-resize*
sudo rm -rf /root/euca2ools*
sudo rm -rf /var/lib/cloud/instance /var/lib/cloud/instances/i*
sudo rm ~/.ssh/*
sudo echo -e ' ' > /home/ubuntu/.ssh/authorized_keys

rm /home/ubuntu/build-dependencies.sh

# Specific to ubuntu
sudo apt-get -y upgrade
sudo apt-get -y dist-upgrade
sudo apt-get -y autoremove
