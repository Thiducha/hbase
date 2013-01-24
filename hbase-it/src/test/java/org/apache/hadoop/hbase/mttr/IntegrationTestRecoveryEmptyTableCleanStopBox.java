package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * Clean stop: MTTR should be minimal, as a clean stop includes closing properly the regions.
 */
/*@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableCleanStopBox
    extends AbstractIntegrationTestRecovery {

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.stop(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
  }
}
  */