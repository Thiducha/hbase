package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

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

    // Start a new one. This ensure that there will be enough DN to write.
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);
    dhc.waitForDatanodesRegistered(4);
  }

  @Override
  protected void kill(String hostname) throws Exception {
    hcm.unplug(willDieBox);
  }

  @Override
  protected void validate(long failureDetectedTime, long failureFixedTime) {
    // Thanks to the stale mode in HDFS, the recovery will be minimal: we won't go to the dead DN
    performanceChecker.logAndCheck(failureFixedTime, getMttrSmallTime());
  }
}
