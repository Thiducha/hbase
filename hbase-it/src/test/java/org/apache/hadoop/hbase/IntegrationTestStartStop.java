package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.PerformanceChecker;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Check that we can stop a HBase cluster. We don't stop HDFS for this one.
 * We launch as many node as we can, then create a table, then loop on:
 * stop cluster / start cluster.
 */
@Category(IntegrationTests.class)
public class IntegrationTestStartStop {
  protected static final Log LOG = LogFactory.getLog(IntegrationTestStartStop.class);

  protected static IntegrationTestingUtility util;
  protected HBaseCluster dhc;
  protected ClusterManager hcm;
  protected static PerformanceChecker performanceChecker;
  protected final String tableName = this.getClass().getName();
  protected static final String COLUMN_NAME = "f";
  private int regionCount;


  private String mainBox;
  private final List<String> boxes = new ArrayList<String>();

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration c = HBaseClusterManager.createHBaseConfiguration();
    Assert.assertTrue(c.getBoolean("hbase.cluster.distributed", false));
    IntegrationTestingUtility.setUseDistributedCluster(c);
    util = new IntegrationTestingUtility(c);
    performanceChecker = new PerformanceChecker(util.getConfiguration());
  }

  private void createTable() throws Exception {
    long startTime, endTime;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    RegionSplitter.SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(regionCount);

    LOG.info(String.format("Creating table %s with %d splits.", tableName, regionCount));
    startTime = System.currentTimeMillis();
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(desc, splits);

    while (util.getHBaseAdmin().getClusterStatus().getRegionsCount() != regionCount + 2) {
      LOG.info("Region count = " + util.getHBaseAdmin().getClusterStatus().getRegionsCount() +
          " on " + regionCount + 2);
      Thread.sleep(1000);
    }
    waitForNoTransition();

    endTime = System.currentTimeMillis();

    LOG.info("Pre-split table created successfully in " + (endTime - startTime) +" ms");
  }

  private void waitForNoTransition() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();

    while (!admin.getClusterStatus().getRegionsInTransition().isEmpty()) {
      Thread.sleep(200);
    }
  }


  protected void setBoxes() {
    mainBox = System.getenv("HBASE_IT_BOX_0");

    for (int nbBox = 1; ; nbBox++) {
      String curBox = System.getenv("HBASE_IT_BOX_" + nbBox);
      if (curBox != null) {
        boxes.add(curBox);
      } else {
        break;
      }
    }
  }


  protected void genericStart() throws Exception {
    setBoxes();
    assert boxes.size() > 2 : "We need at least 3 boxes with the standard dfs replication";

    // Initialize an empty cluster. We will start all services where we want to start them.
    util.initializeCluster(0);
    dhc = util.getHBaseClusterInterface();
    hcm = util.createClusterManager();

    hcm.checkAccessible(mainBox);
    hcm.killAllServices(mainBox);
    hcm.rmHDFSDataDir(mainBox);

    for (String box : boxes) {
      hcm.checkAccessible(box);
      hcm.killAllServices(box);
      hcm.rmHDFSDataDir(box);
    }

    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    hcm.formatNameNode(mainBox); // synchronous

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    for (String box : boxes) {
      hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, box);
    }
    dhc.waitForDatanodesRegistered(boxes.size());
  }


  protected void genericStop() throws IOException {
    hcm.killAllServices(mainBox);
    hcm.rmHDFSDataDir(mainBox);
    for (String box : boxes) {
      hcm.killAllServices(box);
      hcm.rmHDFSDataDir(box);
    }
  }

  private void startHBase() throws Exception {
    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);
    for (String box : boxes) {
      hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, box);
    }
    LOG.info("Waiting for the master to be up");
    dhc.waitForActiveAndReadyMaster();

    LOG.info("Waiting for meta & root regions to be online");
    while (!dhc.waitForActiveAndReadyMaster() ||
        util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 2) {
      Thread.sleep(200);
    }

    while (util.getHBaseAdmin().getClusterStatus().getServersSize() != boxes.size()) {
      LOG.info("Waiting for the region servers, got " +
          util.getHBaseAdmin().getClusterStatus().getServersSize() + " on " + boxes.size());
      Thread.sleep(2000);
    }
    LOG.info("Got all RS, now waiting for an empty RIT");
    waitForNoTransition();
  }

  private void waitForHBaseServersDeath() throws Exception {
    long waitTime = 60 * 1000 * 5;
    long startTime = System.currentTimeMillis();
    HConnectionManager.createConnection(hcm.getConf()).close();
    HConnectionManager.createConnection(dhc.getConf()).close();
    for (String box : boxes) {
      while (hcm.isRunning(ClusterManager.ServiceType.HBASE_REGIONSERVER, box)) {
        LOG.info("Waiting for rs on " + box + " to die");
        performanceChecker.check(System.currentTimeMillis() - startTime, waitTime);
        Thread.sleep(2000);
      }
    }

    while (hcm.isRunning(ClusterManager.ServiceType.HBASE_MASTER, mainBox)) {
      LOG.info("Waiting for master on " + mainBox + " to die");
      performanceChecker.check(System.currentTimeMillis() - startTime, waitTime);
      Thread.sleep(2000);
    }
    performanceChecker.logAndCheck(System.currentTimeMillis() - startTime, waitTime);
  }

  @Test
  public void testStartStop() throws Exception {
    genericStart();
    regionCount = Math.max(50, 10 * boxes.size());

    // First start: we create the tables only
    startHBase();
    createTable();
    for(int i=1;i<10; i++){ Thread.sleep(1000000L); }
    util.getHBaseAdmin().shutdown();
    waitForHBaseServersDeath();

    // Then the iterations
    for (int i = 0; i < 100; i++) {
      LOG.info("start stop iteration " + i + " starting hbase");
      startHBase();
      LOG.info("start stop iteration " + i + " hbase started, doing something it");
      doSomething();
      LOG.info("start stop iteration " + i + " hbase started, stopping it");
      util.getHBaseAdmin().shutdown();
      LOG.info("start stop iteration " + i + " hbase shutdown done, waiting for servers to stop");
      waitForHBaseServersDeath();
    }

    genericStop();
  }


  /**
   * Can be overridden if we want to create specific conditions (split in progress, ...)
   * before stopping the cluster
   */
  protected void doSomething() {
  }
}