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
    int nbDN = 0;
    for (int nbBox = 1; ; nbBox++) {
      String curBox = System.getenv("HBASE_IT_BOX_" + nbBox);
      if (curBox != null) {
        hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, curBox);
        nbDN++;
      } else {
        break;
      }
    }
    dhc.waitForDatanodesRegistered(nbDN);

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    for (int nbBox = 1; ; nbBox++) {
      String curBox = System.getenv("HBASE_IT_BOX_" + nbBox);
      if (curBox != null) {
        hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, curBox);
      } else {
        break;
      }
    }
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
  }

  @Override
  protected void kill(String hostname) throws Exception {
  }
}
