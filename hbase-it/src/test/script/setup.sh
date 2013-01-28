# you need ssh to be configured on all machines.
#
#!/bin/bash
ORIG_HBASE_DIR=~/dev/hbase
ORIG_HDFS_DIR=~/dev/hadoop-common

ORIG_CONF=$ORIG_HBASE_DIR/hbase-it/src/test/resources/

HBASE_REP=~/tmp-recotest/hbase

#the dev directory for hdfs (1.x or 2.x)
HDFS_REP=~/tmp-recotest/hadoop-common
CONF_DIR=~/tmp-recotest/conf


for CBOX in $*; do
  echo "Doing a first ssh to the box to get it registered - $CBOX"
  echo "Now doing ssh to ensure the boxes are recognized between themselves"
  for CBOX2 in $*; do
    ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $CBOX2 'echo ssh ok from $CBOX to $CBOX2'"
  done
  ssh -A -o StrictHostKeyChecking=no 127.0.0.1 'echo 127.0.0.1 ssh ok'
  ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $HBASE_IT_MAIN_BOX 'echo ssh ok from $CBOX to $HBASE_IT_MAIN_BOX'"
done

echo "preparing working data dir. If the tmp-recotest exists, we keep it, but we delete the data dir"
mkdir -p ~/tmp-recotest
rm -rf ~/tmp-recotest/data
rm -rf ~/tmp-recotest/hbase/logs/*

echo "updating the local tmp-recotest with hdfs & hbase dirs content"
rsync -az --delete $ORIG_HBASE_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src' --exclude classes --exclude test-classes
rsync -az --delete $ORIG_HDFS_DIR  ~/tmp-recotest --exclude '.git' --exclude 'src' --exclude classes --exclude test-classes

echo "preparing conf dirs"
mkdir -p $CONF_DIR/conf-hadoop

export HBASE_IT_MAIN_BOX=`hostname`
echo The main box will be $HBASE_IT_MAIN_BOX - this named must be resolvable from all other boxes

sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml >  $CONF_DIR/conf-hadoop/core-site.xml
sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml $ORIG_CONF/it-hdfs-site.xml > $CONF_DIR/conf-hadoop/core-hdfs.xml

sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-core-site.xml  >  $HBASE_REP/conf/core-site.xml
sed 's/HBASE_IT_MAIN_BOX/'$HBASE_IT_MAIN_BOX'/g' $ORIG_CONF/it-hbase-site.xml > $HBASE_REP/conf/hbase-site.xml


echo ready - now copying temp-recotest to the first box
#We copy to the fist box then from this one to the others for AWS like stuff, when we're going to a remote cluster
ssh $1 "mkdir -p tmp-recotest"
rsync -az --delete ~/tmp-recotest/* $1:tmp-recotest




echo now copying the hbase and hdfs directories
for CBOX in $*; do
  echo "copying from $1 to $CBOX"
  ssh -o StrictHostKeyChecking=no $CBOX "mkdir -p tmp-recotest"
  ssh -o StrictHostKeyChecking=no $CBOX "rm -rf tmp-recotest/data"
  ssh -A $1 "rsync -az --delete tmp-recotest/* $CBOX:tmp-recotest/"
  rsync -az ~/.m2/* $CBOX:.m2
done

echo "export HBASE_IT_MAIN_BOX=$HBASE_IT_MAIN_BOX" > ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_WILLDIE_BOX=$1"        >> ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_WILLSURVIVE_BOX=$2"    >> ~/tmp-recotest/local.env.tosource
echo "export HBASE_IT_LATE_BOX=$3"           >> ~/tmp-recotest/local.env.tosource

export HBASE_IT_MAIN_BOX=$HBASE_IT_MAIN_BOX
export HBASE_IT_WILLDIE_BOX=$1
export HBASE_IT_WILLSURVIVE_BOX=$2
export HBASE_IT_LATE_BOX=$3

echo "done"
