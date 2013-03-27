/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mttr;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HBaseCluster;
import org.apache.hadoop.hbase.HBaseClusterManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.PerformanceChecker;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * An integration test to measure the time needed to recover when we lose a regionserver.
 * Makes sense only with a distributed cluster.
 * <p/>
 * <p/>
 * We try to use default config as much as possible.
 * So:
 * We need 4 boxes, because we have a replication factor of 3. If we kill a box, we still need
 * 3 live boxes to make keep happy.
 * We have a single ZK, on the master box. This partly minimizes the ZK cost. Partly, because
 * this box is also a DN, so ZK with fight with the DN for i/o resources.
 * <p/>
 * Scenario:
 * - start 3 boxes
 * - create a table
 * - have all regions of this table on a single one
 * - start another box
 * - kill the region server or the box with all regions
 * - measure time.
 * <p></p>
 * The cluster must be already deployed, i.e. the right packages must have been copied, the ssh must be
 * working between the different boxes, ...
 * <p></p>
 * <p/>
 * The following env variable must be defined:
 * HBASE_HOME
 * HADOOP_HOME
 * HADOOP_CONF_DIR
 * <p></p>
 * The boxes are defined with the following env variables:
 * HBASE_IT_BOX_0
 * HBASE_IT_BOX_1
 * HBASE_IT_BOX_2
 * HBASE_IT_BOX_3
 */

public abstract class AbstractIntegrationTestRecovery {
  public static final String MTTR_SMALL_TIME_KEY = "hbase-it.mttr.small.time";
  public static final long MTTR_SMALL_TIME_DEFAULT = 25000;

  public static final String MTTR_LARGE_TIME_KEY = "hbase-it.mttr.large.time";
  public static final long MTTR_LARGE_TIME_DEFAULT = 10000000;

  protected String mainBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_0");
  protected String willDieBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_1");
  protected String willSurviveBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_2");
  protected String lateBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_3");

  protected static final Log LOG
      = LogFactory.getLog(AbstractIntegrationTestRecovery.class);

  /**
   * Table used for the tests.
   */
  protected final String tableName = this.getClass().getSimpleName();
  protected static final String CF = "f";
  protected final int regionCount;


  protected static IntegrationTestingUtility util;
  protected HBaseCluster dhc;
  protected ClusterManager hcm;
  protected static PerformanceChecker performanceChecker;

  /**
   * This boolean allows to set the test as destructive (it will remove the data) or not.
   * When not destructive, the test will no touch the data on the cluster (it will kill nodes
   * however, so if there is a bug in the recovery you may lose data).
   */
  protected static boolean destructiveTest = true;


  /**
   * Default constructor, creates a table with 10 regions.
   */
  protected AbstractIntegrationTestRecovery() {
    this(10);
  }

  /**
   * @param regCount the number of regions of the table created.
   */
  protected AbstractIntegrationTestRecovery(int regCount) {
    regionCount = regCount;
  }


  @BeforeClass
  public static void setUp() throws Exception {
    Configuration c = HBaseClusterManager.createHBaseConfiguration();

    Assert.assertTrue(c.getBoolean("hbase.cluster.distributed", false));

    IntegrationTestingUtility.setUseDistributedCluster(c);
    util = new IntegrationTestingUtility(c);

    performanceChecker = new PerformanceChecker(c);

    String s = System.getenv("HBASE_IT_DESTRUCTIVE_TEST");
    if (s != null && Boolean.parseBoolean(s)) {
      destructiveTest = true;
    }
  }

  private void createTable() throws Exception {
    long startTime, endTime;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(CF));
    RegionSplitter.SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(regionCount);

    LOG.info(String.format("Creating table %s with %d splits.", tableName, regionCount));
    startTime = System.currentTimeMillis();
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(desc, splits);

    while (util.getHBaseAdmin().getClusterStatus().getRegionsCount() != regionCount + 1) {
      Thread.sleep(1000);
    }
    waitForNoTransition();

    endTime = System.currentTimeMillis();

    LOG.info("Pre-split table created successfully in " + (endTime - startTime) + "ms");
  }

  protected void waitForNoTransition() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();

    do {
      Thread.sleep(200);
    } while (!admin.getClusterStatus().getRegionsInTransition().isEmpty());
  }


  /**
   * Initialize an empty cluster. We will start all services where we want to start them.
   * Check that we can connect to the servers
   * Kill the services running on them
   * if the 'destructive test' flag is set, delete the existing data
   */
  protected void prepareCluster() throws Exception{
    util.initializeCluster(0);
    dhc = util.getHBaseClusterInterface();
    hcm = util.createClusterManager();

    // In case we stopped the previous test while is was not connected
    hcm.replug(willDieBox);

    hcm.checkAccessible(mainBox);
    hcm.checkAccessible(willDieBox);
    hcm.checkAccessible(willSurviveBox);
    hcm.checkAccessible(lateBox);

    hcm.killAllServices(mainBox);
    hcm.killAllServices(willDieBox);
    hcm.killAllServices(willSurviveBox);
    hcm.killAllServices(lateBox);

    if (destructiveTest) {
      hcm.rmHDFSDataDir(mainBox);
      hcm.rmHDFSDataDir(willDieBox);
      hcm.rmHDFSDataDir(willSurviveBox);
      hcm.rmHDFSDataDir(lateBox);
    }
  }

  /**
   * Do the following tasks, in this order:
   * start the cluster, with:
   *  main box: 1 ZooKeeper, 1 Namenode, 1 Master, 1 Region Server, 1 Datanode
   *  willDieBox: 1 1 region server
   *  willSurviveBox: 1 datanode
   *  lateBox: 1 datanode
   */
  protected void genericStart() throws Exception {
    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    if (destructiveTest) {
      hcm.formatNameNode(mainBox); // synchronous
    }

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    dhc.waitForDatanodesRegistered(3);

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    // We want meta & root on the main server, so we start only one RS at the beginning

    while (!dhc.waitForActiveAndReadyMaster() ||
        util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 1) {
      Thread.sleep(200);
    }

    // No balance please
    util.getHBaseAdmin().setBalancerRunning(false, true);

    // We can now start the second rs
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
    while (util.getHBaseAdmin().getClusterStatus().getServersSize() != 1) {
      Thread.sleep(200);
    }

    // Now we have 2 region servers and 3 datanodes.
  }


  public long getMttrSmallTime() {
    return hcm.getConf().getLong(MTTR_SMALL_TIME_KEY, MTTR_SMALL_TIME_DEFAULT);
  }

  public long getMttrLargeTime() {
    return hcm.getConf().getLong(MTTR_LARGE_TIME_KEY, MTTR_LARGE_TIME_DEFAULT);
  }


  protected void moveToSecondRSSync() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    ServerName mainSN = dhc.getServerHoldingMeta();
    int toMove = 0;

    for (HRegionInfo hri : admin.getTableRegions(tableName.getBytes())) {
      if (dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
        admin.move(hri.getEncodedNameAsBytes(), null);
        toMove++;
      }
    }

    // wait for the moves to be done
    waitForNoTransition();

    // Check that the regions have been moved
    admin.getConnection().clearRegionCache();
    int remaining = 0;
    for (HRegionInfo hri : admin.getTableRegions(tableName.getBytes())) {
      if (dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
        remaining++;
      }
    }

    assert remaining == 0 : "Some regions are still on " + mainSN.getHostAndPort() +
        "! remaining=" + remaining + ", toMove=" + toMove;
  }

  @Test
  public void testKillRS() throws Exception {
    prepareCluster();
    beforeStart();
    genericStart();
    Configuration localConf = new Configuration(util.getConfiguration());

    HBaseAdmin admin = util.getHBaseAdmin();

    createTable();
    localConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HTable htable = new HTable(localConf, tableName);

    // now moving all the regions on the region server we're going to kill
    moveToSecondRSSync();

    beforeKill();

    // Now killing
    final long startTime;
    final long failureDetectedTime;
    final long failureFixedTime;

    final int nbReg = dhc.getClusterStatus().getRegionsCount();

    kill(willDieBox);
    try {
      // Check that the RS is really killed. We do that by trying to connect to the server with
      //  a minimum connect timeout. If it succeeds, it means it's still there...
      int rsport = util.getConfiguration().getInt("hbase.regionserver.port", 60020);
      while (ClusterManager.isReachablePort(willDieBox, rsport)) {
        Thread.sleep(400);
      }

      startTime = System.currentTimeMillis();

      afterKill();

      // How long does it take to discover that we need to do something?
      while (admin.getClusterStatus().getDeadServers() == 0) {
        Thread.sleep(200);
      }
      failureDetectedTime = System.currentTimeMillis();
      LOG.info("Detection took: " + (failureDetectedTime - startTime));
      afterDetection();

      // Now, how long does it take to recover?
      boolean ok = false;
      do {
        waitForNoTransition();
        ok =  dhc.getClusterStatus().getRegionsCount() == nbReg;
      } while (!ok);

      failureFixedTime = System.currentTimeMillis();
      LOG.info(("Failure fix took: " + (failureFixedTime - failureDetectedTime)));

      afterRecovery();

    } finally {
      // If it was an unplug, we replug it now
      htable.close();
      hcm.replug(willDieBox);
    }


    validate((failureDetectedTime - startTime), (failureFixedTime - failureDetectedTime));

    if (!destructiveTest) {
      util.getHBaseAdmin().deleteTable(tableName);
    }
  }

  /**
   * Called before the cluster start.
   *
   * @throws Exception
   */
  protected void beforeStart() throws Exception {
  }

  /**
   * Called just before the kill; when the cluster is up and running.
   *
   * @throws Exception
   */
  protected void beforeKill() throws Exception {
  }

  /**
   * Called just after the kill.
   *
   * @throws Exception
   */
  protected void afterKill() throws Exception {
  }

  /**
   * Kill the RS. It's up to the implementer to choose the mean (kill 15, 9, box unplugged...
   *
   * @param hostname the box to kill
   * @throws Exception
   */
  protected abstract void kill(String hostname) throws Exception;

  /**
   * Called once we detected that the RS is dead (i.e. the ZK node expired)
   *
   * @throws Exception
   */
  protected void afterDetection() throws Exception {
  }

  /**
   * Called once the recovery is ok.
   */
  protected void afterRecovery() throws Exception {
  }

  /**
   * Called at the end to validate the results
   *
   * @param failureDetectedTime the time for detection
   * @param failureFixedTime    the time for fixing the failure
   * @throws Exception
   */
  protected void validate(long failureDetectedTime, long failureFixedTime) throws Exception {
  }
}
