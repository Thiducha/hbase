# you need ssh to be configured on all machines.
#
#!/bin/bash
ORIG_HBASE_DIR=`readlink -f ..`
ORIG_HDFS_DIR=`readlink -f $ORIG_HBASE_DIR/../hadoop-common`

ORIG_CONF=$ORIG_HBASE_DIR/hbase-it/src/test/resources/

echo "HBase source is $ORIG_HBASE_DIR"
echo "Hadoop source is $ORIG_HDFS_DIR"


HBASE_REP=~/tmp-recotest/hbase
#the dev directory for hdfs (1.x or 2.x)
HDFS_REP=~/tmp-recotest/hadoop-common
CONF_DIR=~/tmp-recotest/conf

HOST_NAME=`hostname`
export HBASE_IT_MAIN_BOX=$HOST_NAME

echo "Now doing ssh to ensure the boxes are recognized between themselves"
for CBOX in $* $HOST_NAME 127.0.0.1; do
  for CBOX2 in $* $HOST_NAME; do
    ssh -A -o StrictHostKeyChecking=no $CBOX2 "ssh -o StrictHostKeyChecking=no $CBOX 'echo on $CBOX2 ssh ok to / from $CBOX / $CBOX2'"  &
  done
done

wait

echo "preparing working data dir. If the tmp-recotest exists, we keep it, but we delete the data dir"
mkdir -p ~/tmp-recotest
rm -rf ~/tmp-recotest/data
rm -rf ~/tmp-recotest/hbase/logs/*

echo "updating the local tmp-recotest with hdfs & hbase dirs content"
rsync -az --delete $ORIG_HBASE_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src' --exclude classes --exclude test-classes --exclude 'hbase/hbase-*' --exclude '*.jar' --exclude '.idea'
rsync -az --delete $ORIG_HDFS_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src' --exclude classes --exclude test-classes  --exclude '*.html' --exclude '*-sources.jar'  --exclude '.idea'


echo "preparing conf dirs"
mkdir -p $CONF_DIR/conf-hadoop


echo The main box will be $HBASE_IT_MAIN_BOX

sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml >  $CONF_DIR/conf-hadoop/core-site.xml
sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml $ORIG_CONF/it-hdfs-site.xml > $CONF_DIR/conf-hadoop/core-hdfs.xml

sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml  >  $HBASE_REP/conf/core-site.xml
sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-hbase-site.xml > $HBASE_REP/conf/hbase-site.xml


echo "Copying the libs we need locally"
mkdir -p ~/tmp-recotest/hbase
mkdir -p ~/tmp-recotest/hbase/lib

for LIB in `cat $ORIG_HBASE_DIR/target/cached_classpath.txt | tr ':' '\n' `
 do
 cp $LIB ~/tmp-recotest/hbase/lib
done


echo "$HOME/tmp-recotest/hbase/lib/*.jar" >  ~/tmp-recotest/hbase/target/cached_classpath.txt

find . -name "*.jar" | xargs -n 1 -i  mv {}   ~/tmp-recotest/hadoop-common/lib/


echo now copying tmp-recotest dirs to all boxes, deleting the data dir if any
for CBOX in $*; do
  echo "copying from $1 to $CBOX"
  ssh -o StrictHostKeyChecking=no $CBOX "mkdir -p tmp-recotest"
  ssh -o StrictHostKeyChecking=no $CBOX "rm -rf tmp-recotest/data"
  rsync -az --delete ~/tmp-recotest/* $CBOX:tmp-recotest/
done

echo export

echo "export HBASE_IT_MAIN_BOX=$HBASE_IT_MAIN_BOX" > ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_WILLDIE_BOX=$1"        >> ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_WILLSURVIVE_BOX=$2"    >> ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_LATE_BOX=$3"           >> ~/tmp-recotest/local.env.tosource

echo "export HBASE_HOME=$HOME/tmp-recotest/hbase"   >> ~/tmp-recotest/local.env.tosource
echo "export HADOOP_HOME=$HOME/tmp-recotest/hadoop-common"   >> ~/tmp-recotest/local.env.tosource
echo "export HADOOP_CONF_DIR=$HOME/tmp-recotest/conf/conf-hadoop"   >> ~/tmp-recotest/local.env.tosource

echo "done"
