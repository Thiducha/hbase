package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTests.class)
public class IntegrationTestLaunchYSCBCluster extends AbstractIntegrationTestRecovery {

  protected void start() throws Exception {
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
  }


  @Override
  @Test
  public void testKillServer() throws Exception {
    prepareCluster();
    start();
    HBaseAdmin admin = util.getHBaseAdmin();

    HTableDescriptor desc = new HTableDescriptor("usertable");
    desc.addFamily(new HColumnDescriptor("family"));
    admin.createTable(desc);
    while (true)Thread.sleep(10000);
  }

  @Override
  protected void kill(String hostname) throws Exception {
  }
}
