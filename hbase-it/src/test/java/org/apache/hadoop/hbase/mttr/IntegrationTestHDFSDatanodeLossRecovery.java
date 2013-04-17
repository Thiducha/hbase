package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.experimental.categories.Category;

/**
 * Test where we unplug a datanode on another computer than the used RS and check that the nex
 * operations on the RS are not taking too much time.
 * Results: ~1 minutes or instantaneous, this last result beeing abnormal.
 */
@Category(IntegrationTests.class)
public class IntegrationTestHDFSDatanodeLossRecovery extends AbstractIntegrationTestHDFSRecovery {

  /**
   * Write a put on all regions. This will need to be recovered.
   *
   * @throws Exception
   */
  @Override
  protected void beforeKill() throws Exception {
    HTable t = new HTable(util.getConfiguration(), tableName);

    for (HRegionInfo hri : t.getRegionLocations().keySet()) {
      byte[] key = hri.getStartKey();
      if (key != null && key.length > 0) {
        Put p = new Put(key);
        p.add(CF.getBytes(), key, key);
        t.put(p);
      }
    }
    t.close();

    // Start a new one. This ensure that there will be enough DN to write.
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);
    dhc.waitForDatanodesRegistered(nbDN + 1);
  }

  @Override
  protected void kill(String hostname) throws Exception {
    hcm.unplug(hostname);
  }

  @Override
  protected void afterKill() throws Exception {
    HTable t = new HTable(util.getConfiguration(), tableName);

    long start = System.currentTimeMillis();

    for (HRegionInfo hri : t.getRegionLocations().keySet()) {
      byte[] key = hri.getStartKey();
      if (key != null && key.length > 0) {
        Put p = new Put(key);
        p.add(CF.getBytes(), key, key);
        t.put(p);
      }
    }

    long stop = System.currentTimeMillis();
    long result = stop - start;

    t.close();

    LOG.info("time to do the second put : " + result + " ms");
    performanceChecker.logAndCheck(result, 60 * 1000 + getMttrSmallTime());
  }
}
