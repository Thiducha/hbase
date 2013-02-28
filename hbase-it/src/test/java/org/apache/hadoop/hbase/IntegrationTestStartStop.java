package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
 * stop cluster
 * start cluster
 * do puts
 */
@Category(IntegrationTests.class)
public class IntegrationTestStartStop {
  protected static final Log LOG
      = LogFactory.getLog(IntegrationTestStartStop.class);

  protected static IntegrationTestingUtility util;
  protected HBaseCluster dhc;
  protected ClusterManager hcm;
  protected static PerformanceChecker performanceChecker;
  protected final String tableName = this.getClass().getName();
  protected static final String COLUMN_NAME = "f";
  private int regionCount = 50;

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
      Thread.sleep(1000);
    }
    waitForNoTransition();

    endTime = System.currentTimeMillis();

    LOG.info(String.format("Pre-split table created successfully in %dms.", (endTime - startTime)));
  }

  private void waitForNoTransition() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();

    while (!admin.getClusterStatus().getRegionsInTransition().isEmpty()) {
      Thread.sleep(200);
    }
  }

  private String mainBox;
  private final List<String> boxes = new ArrayList<String>();

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
    while (util.getHBaseAdmin().getClusterStatus().getServersSize() != boxes.size()) {
      Thread.sleep(200);
    }
    waitForNoTransition();
  }

  private void waitForRegionServerDead() throws InterruptedException {
    long waitTime = 60 * 1000 * 5;
    long startTime = System.currentTimeMillis();
    int rsport = util.getConfiguration().getInt("hbase.regionserver.port", 60020);
    for (String box : boxes) {
      while (ClusterManager.isReachablePort(box, rsport)) {
        performanceChecker.check(System.currentTimeMillis(), startTime + waitTime);
        Thread.sleep(400);
      }
    }

    int masterPort = util.getConfiguration().getInt("hbase.master.port", 60000);
    while (ClusterManager.isReachablePort(mainBox, masterPort)) {
      performanceChecker.check(System.currentTimeMillis(), startTime + waitTime);
      Thread.sleep(400);
    }
  }


  @Test
  public void testStartStop() throws Exception {
    genericStart();
    regionCount = Math.max(50, 10 * boxes.size());

    // First start: we create the tables only
    startHBase();
    createTable();
    util.getHBaseAdmin().shutdown();

    for (int i = 0; i < 100; i++) {
      startHBase();
      util.getHBaseAdmin().shutdown();
      doSomething();
      waitForRegionServerDead();
    }

    genericStop();
  }


  /**
   * Can be sublclassed if we want to create specific condition before stopping the cluster
   */
  protected void doSomething() {
  }
}