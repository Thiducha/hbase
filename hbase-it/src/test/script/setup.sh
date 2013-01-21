#!/bin/bash
ORIG_HBASE_DIR=~/dev/hbase
ORIG_HDFS_DIR=~/dev/hadoop-common

BOX2=192.168.1.12
BOX3=192.168.1.13
BOX4=192.168.1.15


ORIG_CONF=$ORIG_HBASE_DIR/hbase-it/src/test/resources/

HBASE_REP=~/tmp-recotest/hbase
HDFS_REP=~/tmp-recotest/hadoop-common
CONF_DIR=~/tmp-recotest/conf

#the dev directory for hdfs (1.x or 2.x)


echo "preparing working data dir. If the tmp-recotest exists, we keep it, but we delete the data dir"
mkdir -p ~/tmp-recotest
rm -rf ~/tmp-recotest/data
mkdir ~/tmp-recotest/data

echo "updating the local tmp-recotest with hdfs & hbase dirs content"
rsync -az --delete $ORIG_HBASE_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src' --exclude dev-support
rsync -az --delete $ORIG_HDFS_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src'

mkdir -p $CONF_DIR/conf-hadoop
cp $ORIG_CONF/it-core-site.xml $CONF_DIR/conf-hadoop/core-site.xml
cp $ORIG_CONF/it-hdfs-site.xml $CONF_DIR/conf-hadoop/core-hdfs.xml

cp $ORIG_CONF/it-core-site.xml $HBASE_REP/conf/core-site.xml
cp $ORIG_CONF/it-hbase-site.xml $HBASE_REP/conf/hbase-site.xml

echo ready - now copying maven repos
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX2:~/.m2 &
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX3:~/.m2 &
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX4:~/.m2 &

wait

echo sync .m2 done, now copying the hbase and hdfs directories
rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX2:~/tmp-recotest/ &
rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX3:~/tmp-recotest/ &
rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX4:~/tmp-recotest/ &

wait
