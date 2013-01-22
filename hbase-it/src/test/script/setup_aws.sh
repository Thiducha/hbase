#!/bin/bash

# we need the JAVA and MAVEN binaries to be available in ~/soft
# we will install them in /opt
JAVA=jdk-6u38-linux-x64.bin
JAVAS=jdk1.6.0_38

MAVEN=apache-maven-3.0.4-bin.tar.gz
MAVENS=apache-maven-3.0.4

BOX1=$1

echo "the $BOX1 will contain the hbase source code to run mvn it tests"

MAVEN_DIR=~/.m2

for CBOX in $*; do
  echo "Doing a first ssh to the box to get it registered - $CBOX"
  ssh -o StrictHostKeyChecking=no $CBOX 'echo yo man'

  echo installing java from sun
  scp ~/soft/$JAVA $CBOX:
  ssh $CBOX "chmod oug+x $JAVA; yes | ~/$JAVA"
  ssh $CBOX "mv ~/$JAVAS /opt/jdk1.6"

  echo creating maven repo
  ssh $CBOX "mkdir -p ~/.m2"

  echo "Now doing ssh to ensure the boxes are recognized between themselves - pipelening 'yes' does not work"
  ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $BOX1 'echo yo man'"
  ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $BOX2 'echo yo man'"
  ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $BOX3 'echo yo man'"
  ssh -A $CBOX "ssh -o StrictHostKeyChecking=no $BOX4 'echo yo man'"
done

echo copying hbase src on box 1
ssh $BOX1 "mkdir -p ~dev; mkdir -p ~dev/hbase"
rsync -az --delete ~/dev/hbase $BOX1:/dev/hbase --exclude '.git'

echo installing maven on box1 - redhat does not have wget by default
scp ~/soft/$MAVEN $BOX1:
ssh $BOX1 "tar xvf ~/$MAVEN"
ssh $BOX1 "mv ~/$MAVENS /opt/apache-maven"

echo "Now doing the global setup"
./setup.sh $*

echo "We don't need to set the sticky bits on dev-support firewall config here: we're root on aws"

echo "we're done."