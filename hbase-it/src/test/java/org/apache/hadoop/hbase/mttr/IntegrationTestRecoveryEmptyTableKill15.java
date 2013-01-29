package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * On a kill 15, we rely on the shutdown hooks included in the region server. These hooks will
 * cancel our lease to ZK, so the recovery will start immediately.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableKill15 extends AbstractIntegrationTestRecovery {


  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.signal(ClusterManager.ServiceType.HBASE_REGIONSERVER, "TERM", willDieBox);
  }
}
