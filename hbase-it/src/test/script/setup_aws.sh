#!/bin/bash

# we need the JAVA and MAVEN binaries to be available in ~/soft
# we will install them in /opt
JAVA=jdk-6u38-linux-x64.bin
JAVAS=jdk1.6.0_38

MAVEN=apache-maven-3.0.4-bin.tar.gz
MAVENS=apache-maven-3.0.4

BOX1=root@$1

echo "the $BOX1 will contain the hbase source code to run mvn it tests"

for CBOX in $*; do
  RCBOX=root@$CBOX

  if ssh -o StrictHostKeyChecking=no $RCBOX "ls $JAVA" >/dev/null 2>/dev/null; then
    echo "it seems $JAVA is already installed"
  else
    echo installing java from sun
    scp ~/soft/$JAVA $RCBOX:
    ssh $RCBOX "chmod oug+x $JAVA; yes | ./$JAVA"
    ssh $RCBOX "mv $JAVAS /opt/jdk1.6"
  fi

  echo creating maven repo and tmp-recotest dir
  ssh $RCBOX "mkdir -p /grid/0/.m2"
  ssh $RCBOX "mkdir -p /grid/0/tmp-recotest"
  ssh $RCBOX "ln -s /grid/0/.m2 .m2"
  ssh $RCBOX "ln -s /grid/0/tmp-recotest tmp-recotest"
done

echo copying hbase src on $BOX1 - you will need to recompile to start the tests and get the maven repo clean
EXCLUDE="--exclude '.idea' --exclude generated-sources --exclude '.git' --exclude '*-sources.jar' --exclude '*-javadoc.jar' --exclude '*.html'"
ssh $BOX1 "mkdir -p dev"
rm -rf ~/dev/hbase/logs/*
rsync -az --delete ~/dev/hbase $BOX1:dev --exclude target $EXCLUDE

echo copying hadoop src on $BOX1 - we want
rsync -az --delete ~/dev/hadoop-common $BOX1:dev --exclude classes --exclude src  $EXCLUDE --exclude "*.java"

echo "We need the maven repo for hadoop as well if we built hadoop"
ssh $BOX1 "mkdir -p .m2; mkdir -p .m2/repository; mkdir -p .m2/repository/org; mkdir -p .m2/repository/org/apache;"
rsync -az ~/.m2/repository/org/apache/hadoop $BOX1:.m2/repository/org/apache

echo installing maven on box1 - redhat does not have wget by default
if ssh -o StrictHostKeyChecking=no $BOX1 "ls $MAVEN" >/dev/null 2>/dev/null; then
  echo "it seems $MAVEN is already installed"
else
  scp ~/soft/$MAVEN $BOX1:
  ssh $BOX1 "tar xvf ~/$MAVEN"
  ssh $BOX1 "mv ~/$MAVENS /opt/apache-maven"
fi

echo "Now  doing the global setup"

echo "export JAVA_HOME=/opt/jdk1.6"          > /tmp/env.tosource
echo "export MAVEN_HOME=/opt/apache-maven"   >> /tmp/env.tosource
echo "PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:\$PATH"    >> /tmp/env.tosource
echo "export HBASE_IT_MAIN_BOX=$1"           >> /tmp/env.tosource
echo "export HBASE_IT_WILLDIE_BOX=$2"        >> /tmp/env.tosource
echo "export HBASE_IT_WILLSURVIVE_BOX=$3"    >> /tmp/env.tosource
echo "export HBASE_IT_LATE_BOX=$4"           >> /tmp/env.tosource
echo "export HBASE_SSH_OPTS='-A'"             >> /tmp/env.tosource

for CBOX in $*; do
  scp /tmp/env.tosource root@$CBOX:tmp-recotest/env.tosource
  ssh root@$CBOX "cat tmp-recotest/env.tosource >> .bashrc"
done

echo "We don't need to set the sticky bits on dev-support firewall config here: we're root on aws"

echo "we're done. You must now run the setup locally on $BOX1 - command: ssh -A $BOX1"