#!/bin/bash
set -ev

echo "Installing RocksDB"
curl -s -L -O "https://github.com/facebook/rocksdb/archive/master.zip"
unzip master.zip
cd rocksdb-master
make shared_lib
sudo make install
cd ..
