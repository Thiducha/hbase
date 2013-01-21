# you need ssh to be configured on all machines.
#
#!/bin/bash
ORIG_HBASE_DIR=~/dev/hbase
ORIG_HDFS_DIR=~/dev/hadoop-common
MAVEN_DIR=~/.m2

BOX2=$1
BOX3=$2
BOX4=$3


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

#todo: this is for HDFS 2 - Comment it for HDFS 1. it's a bug, see HBASE-7637
#rm -rf $HBASE_REP/hbase-hadoop1-compat/target/

mkdir -p $CONF_DIR/conf-hadoop
cp $ORIG_CONF/it-core-site.xml $CONF_DIR/conf-hadoop/core-site.xml
cp $ORIG_CONF/it-hdfs-site.xml $CONF_DIR/conf-hadoop/core-hdfs.xml

cp $ORIG_CONF/it-core-site.xml $HBASE_REP/conf/core-site.xml
cp $ORIG_CONF/it-hbase-site.xml $HBASE_REP/conf/hbase-site.xml

echo ready - now copying maven repos
rsync -az  ${MAVEN_DIR}/* $BOX2:${MAVEN_DIR} &
rsync -az  ${MAVEN_DIR}/* $BOX3:${MAVEN_DIR} &
rsync -az  ${MAVEN_DIR}/* $BOX4:${MAVEN_DIR} &

wait

echo sync .m2 done, now copying the hbase and hdfs directories
rsync -az --delete ~/tmp-recotest/* $BOX2:~/tmp-recotest/ &
rsync -az --delete ~/tmp-recotest/* $BOX3:~/tmp-recotest/ &
rsync -az --delete ~/tmp-recotest/* $BOX4:~/tmp-recotest/ &

wait

echo "done"
