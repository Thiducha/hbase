package org.apache.hadoop.hbase.mttr;

import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * When we unplug a box, we rely on the ZK timeout. So the recovery time will be roughly the
 *  ZK time + split time + assignment time. Here, the table is empty and there are just a
 *  small number of region, so we expect the recovery time to equals the ZK time.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableUnplugBox
    extends AbstractIntegrationTestRecovery {

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }
}
