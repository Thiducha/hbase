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

echo "Install git on $BOX1"
ssh $BOX1 "yum repolist"
ssh $BOX1 "rpm -Uvh http://dl.fedoraproject.org/pub/epel/5/x86_64/epel-release-5-4.noarch.rpm"
ssh $BOX1 "yes | yum install -y git"

echo "Cloning hbase repo on $BOX1"
ssh $BOX1 "mkdir -p ~/dev"
ssh $BOX1 "cd dev ; git clone https://github.com/nkeywal/hbase.git"

echo installing maven on box1 - redhat does not have wget by default
if ssh -o StrictHostKeyChecking=no $BOX1 "ls $MAVEN" >/dev/null 2>/dev/null; then
  echo "it seems $MAVEN is already installed"
else
  scp ~/soft/$MAVEN $BOX1:
  ssh $BOX1 "tar xvf ~/$MAVEN"
  ssh $BOX1 "mv ~/$MAVENS /opt/apache-maven"
fi

echo "Synchronizing the cluster dir on the main box"
ssh $BOX1 "mkdir -p cluster"
rsync  -az --delete ~/cluster/* $BOX1:~/cluster

echo "Now  doing the global setup"

echo "export JAVA_HOME=/opt/jdk1.6"          > /tmp/env.tosource
echo "export MAVEN_HOME=/opt/apache-maven"   >> /tmp/env.tosource
echo "PATH=$JAVA_HOME/bin:$MAVEN_HOME/bin:\$PATH"    >> /tmp/env.tosource

for CBOX in $*; do
  scp /tmp/env.tosource root@$CBOX:tmp-recotest/env.tosource
  ssh root@$CBOX "cat tmp-recotest/env.tosource >> .bashrc"
done

echo "We don't need to set the sticky bits on dev-support firewall config here: we're root on aws"

echo "we're done. You must now run the setup locally on $BOX1 - command: ssh -A $BOX1"
