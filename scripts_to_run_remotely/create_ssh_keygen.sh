#!/bin/bash
echo -e "n" | ssh-keygen -q -t rsa -N "" -f /home/ubuntu/.ssh/id_rsa
chmod 600 /home/ubuntu/.ssh/authorized_keys