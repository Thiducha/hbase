package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * On a kill 9, we cannot rely on the shutdown hooks. On 0.94, we rely on the ZK timeout. On 0.96,
 *  the scripts include a mechanism to cleanup the ZK status when the regionserver or the master
 *  dies. This allows an immediate recovery.
 */
/*@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableKill9
    extends AbstractIntegrationTestRecovery {

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.signal(ClusterManager.ServiceType.HBASE_REGIONSERVER, "KILL", willDieBox);
  }
}
     */