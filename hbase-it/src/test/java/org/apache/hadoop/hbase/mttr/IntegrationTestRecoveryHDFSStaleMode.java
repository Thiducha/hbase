package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * This tests checks that we have the expected behavior with the hdfs stale mode activated.
 * As of March 28th 20013:
 * With hdfs 2.0.3, recovery takes 9 seconds when the stale mode is on, 400 seconds when it's not.
 * With hdfs 1.1.2 the recovery takes around 200 seconds in both cases. We have the same result
 *  in both case because we don't have the write stale mode management in version 1.1, so we're
 *  going to the  dead server. Why it takes half the time in hdfs 1 vs. 2 is another question,
 *  unsolved.
 */
public class IntegrationTestRecoveryHDFSStaleMode extends AbstractIntegrationTestRecovery {

  /**
   * Start a namenode and 3 datanodes.
   */
  protected void genericStart() throws Exception {
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    if (destructiveTest) {
      hcm.formatNameNode(mainBox); // synchronous
    }

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    dhc.waitForDatanodesRegistered(3);

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    // There is no datanode on the main box. This way, when doing the recovery, all the reads will
    //  be done on a remote box. If the stale mode is not active, it will trigger socket timeouts.
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    // We want meta & root on the main server, so we start only one RS at the beginning

    while (!dhc.waitForActiveAndReadyMaster() ||
        util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 1) {
      Thread.sleep(200);
    }

    // No balance please
    util.getHBaseAdmin().setBalancerRunning(false, true);

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
  }


  /**
   * Write a put on all regions. This will need to be recovered.
   * @throws Exception
   */
  @Override
  protected void beforeKill() throws Exception {
    HTable t = new HTable(util.getConfiguration(), tableName);

    for (HRegionInfo hri:t.getRegionLocations().keySet()){
      byte[] key = hri.getStartKey();
      if (key != null && key.length > 0){
        Put p = new Put(key);
        p.add(CF.getBytes(), key, key);
        t.put(p);
      }
    }
    t.close();
    Thread.sleep(30000);

    // Start a new one. This ensure that there will be enough DN to write.
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);
    dhc.waitForDatanodesRegistered(4);
  }

  @Override
  protected void kill(String hostname) throws Exception {
    hcm.unplug(willDieBox);
  }

  /**
   * @param failureDetectedTime the time for detection
   * @param failureFixedTime    the time for fixing the failure
   */
  @Override
  protected void validate(long failureDetectedTime, long failureFixedTime) {
    if (ClusterManager.getEnvNotNull("HADOOP_VERSION").startsWith("1")){
      // In hadoop 1 we can only measure the time spent, but we cannot do any real check.
      performanceChecker.logAndCheck(failureFixedTime, getMttrLargeTime());
    } else {
      performanceChecker.logAndCheck(failureFixedTime, getMttrSmallTime());
    }
  }
}
