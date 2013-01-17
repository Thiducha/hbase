#!/bin/bash
ORIG_HBASE_DIR=~/dev/hbase


ORIG_CONF=$ORIG_HBASE_DIR/hbase-it/src/test/resources/

HBASE_REP=~/tmp-recotest/hbase
HDFS_REP=~/tmp-recotest/hadoop-common
CONF_DIR=~/tmp-recotest/conf

#the dev directory for hdfs (1.x or 2.x)
ORIG_HDFS_DIR=~/dev/hadoop-common



echo "preparing working data dir"
mkdir -p ~/tmp-recotest
rm -rf ~/tmp-recotest/data
mkdir ~/tmp-recotest/data


rsync -az --delete $ORIG_HBASE_DIR  ~/tmp-recotest
rsync -az --delete $ORIG_HDFS_DIR  ~/tmp-recotest

cp $ORIG_CONF/* $CONF_DIR/conf-hadoop
cp $ORIG_CONF/* $HBASE_REP/conf

rm -rf $HBASE_REP/.git
rm -rf $HDFS_REP/.git

echo ready now copy
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX2:~/.m2
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX3:~/.m2
rsync -az -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/.m2/* $BOX4:~/.m2

rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX2:~/tmp-recotest/
rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX3:~/tmp-recotest/
rsync -az --delete -e "ssh  -i $HOME/.ssh/unsecure.priv" ~/tmp-recotest/* $BOX4:~/tmp-recotest/
