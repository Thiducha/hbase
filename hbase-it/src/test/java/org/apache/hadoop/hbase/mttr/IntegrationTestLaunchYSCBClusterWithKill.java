package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.junit.Test;

public class IntegrationTestLaunchYSCBClusterWithKill extends IntegrationTestLaunchYSCBCluster {


  @Override
  @Test
  public void testKillServer() throws Exception {
    super.testKillServer();

    //noinspection InfiniteLoopStatement
    while (true) {
      for (int nbBox = 1; ; nbBox++) {
        Thread.sleep(30L * 60L * 1000L);
        String curBox = System.getenv("HBASE_IT_BOX_" + nbBox);
        if (curBox != null) {
          hcm.kill(ClusterManager.ServiceType.HBASE_REGIONSERVER, curBox);
          hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, curBox);
        } else {
          break;
        }
      }
    }
  }
}
