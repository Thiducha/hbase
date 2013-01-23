#!/bin/bash

# we need the JAVA and MAVEN binaries to be available in ~/soft
# we will install them in /opt
JAVA=jdk-6u38-linux-x64.bin
JAVAS=jdk1.6.0_38

MAVEN=apache-maven-3.0.4-bin.tar.gz
MAVENS=apache-maven-3.0.4

BOX1=$1

echo "the $BOX1 will contain the hbase source code to run mvn it tests"

for CBOX in $*; do
  echo "Doing a first ssh to the box to get it registered - $CBOX"
  echo "Now doing ssh to ensure the boxes are recognized between themselves - pipelening 'yes' does not work"
  for CBOX2 in $*; do
    ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $CBOX2 'echo ssh ok from $CBOX to $CBOX2'"
  done

  if ssh $CBOX "ls $JAVA" >/dev/null; then
    echo "it seems $JAVA is already installed"
  else
    echo installing java from sun
    scp ~/soft/$JAVA $CBOX:
    ssh $CBOX "chmod oug+x $JAVA; yes | $JAVA"
    ssh $CBOX "mv $JAVAS /opt/jdk1.6"
  fi

  echo creating maven repo
  ssh $CBOX "mkdir -p ~/.m2"
done

echo copying hbase src on box 1
ssh $BOX1 "mkdir -p dev"
rsync -az --delete ~/dev/hbase $BOX1:dev --exclude '.git'

echo "We need the maven repo as well if we built hadoop"
rsync -az --delete ~/.m2 root@ec2-54-242-182-117.compute-1.amazonaws.com:

echo installing maven on box1 - redhat does not have wget by default
scp ~/soft/$MAVEN $BOX1:
ssh $BOX1 "tar xvf ~/$MAVEN"
ssh $BOX1 "mv ~/$MAVENS /opt/apache-maven"

echo "Now doing the global setup"
./setup.sh $*


echo "export JAVA_HOME=/opt/jdk1.6"           > /tmp/env
echo "export MAVEN_HOME=/opt/apache-maven"    >> /tmp/env
echo "PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:\$PATH"    >> /tmp/env
echo "export HBASE_IT_WILLDIE_BOX=`echo $2 | cut -c 6-`"         >> /tmp/env
echo "export HBASE_IT_WILLSURVIVE_BOX=`echo $3 | cut -c 6-`"    >> /tmp/env
echo "export HBASE_IT_LATE_BOX=`echo $4 | cut -c 6-`"            >> /tmp/env

scp /tmp/env $BOX1:

echo "We don't need to set the sticky bits on dev-support firewall config here: we're root on aws"

echo "we're done."