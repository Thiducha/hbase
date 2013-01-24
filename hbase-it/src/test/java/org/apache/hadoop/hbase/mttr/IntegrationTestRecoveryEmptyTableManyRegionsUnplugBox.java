package org.apache.hadoop.hbase.mttr;

import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * This test is not useful as it is: we don't have much effect compared to a simple stop.
 */
/*@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableManyRegionsUnplugBox
    extends AbstractIntegrationTestRecovery {

  public IntegrationTestRecoveryEmptyTableManyRegionsUnplugBox(){
    super(1000);
  }

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }
}
    */