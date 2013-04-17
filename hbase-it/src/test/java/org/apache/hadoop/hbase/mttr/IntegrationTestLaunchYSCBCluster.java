package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class IntegrationTestLaunchYSCBCluster extends AbstractIntegrationTestRecovery {

  @Override
  protected void genericStart() throws Exception {
    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    if (destructiveTest) {
      hcm.formatNameNode(mainBox); // synchronous
    }

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    // Number of datanode
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    dhc.waitForDatanodesRegistered(3);

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    // There is no datanode on the main box. This way, when doing the recovery, all the reads will
    //  be done on a remote box. If the stale mode is not active, it will trigger socket timeouts.
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, lateBox);
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);

    while (!dhc.waitForActiveAndReadyMaster() ||
        util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 1) {
      Thread.sleep(200);
    }

    util.getHBaseAdmin().setBalancerRunning(false, true);
  }

  @Override
  protected void kill(String hostname) throws Exception {
    // we don't kill
  }
}
