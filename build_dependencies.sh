#!/bin/bash
#1: TARGET_PATH
cd $1

mkdir petuum
mv scripts.tar.gz petuum/scripts.tar.gz
cd petuum
tar -xf scripts.tar.gz

sudo apt-get update
dpkg --configure -a
sudo apt-get -y install git
sudo apt-get -y install g++ make autoconf git libtool uuid-dev openssh-server cmake libopenmpi-dev openmpi-bin libssl-dev libnuma-dev python-dev python-numpy python-pip python-scipy python-yaml protobuf-compiler subversion libxml2-dev libxslt-dev zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev


# Clone and build Bosen and its dependent repo Third Party
git clone -b stable https://github.com/petuum/bosen.git
cd $1/petuum/bosen
git clone https://github.com/petuum/third_party.git

cd $1/petuum/bosen/third_party
make
cd $1/petuum/bosen
cp defns.mk.template defns.mk
make

# Build any algorithm from bosen here
cd $1/petuum/bosen/app/mlr
sudo make

cd $1/petuum/
git clone https://github.com/eucalyptus/s3cmd
cd $1/petuum/s3cmd
sudo pip install setuptools
sudo python setup.py install

cd $1/petuum/
source build_image_script.sh
