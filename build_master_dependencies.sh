#!/bin/bash
mkdir petuum
mv scripts_for_master.tar.gz petuum/scripts_for_master.tar.gz
cd petuum
tar -xf scripts_for_master.tar.gz

sudo apt-get -y update
sudo dpkg --configure -a
sudo apt-get -y install git
sudo apt-get -y install g++ make autoconf git libtool uuid-dev openssh-server cmake libopenmpi-dev openmpi-bin libssl-dev libnuma-dev python-dev python-numpy python-scipy python-yaml protobuf-compiler subversion libxml2-dev libxslt-dev zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev


# Clone and build Bosen and its dependent repo Third Party
git clone -b stable https://github.com/petuum/bosen.git
cd /home/ubuntu/petuum/bosen
git clone https://github.com/petuum/third_party.git

cd /home/ubuntu/petuum/bosen/third_party
make
cd /home/ubuntu/petuum/bosen
cp defns.mk.template defns.mk
make

# Build any algorithm from bosen here
cd /home/ubuntu/petuum/bosen/app/mlr
make

cd /home/ubuntu/petuum/
source build_image_script.sh
