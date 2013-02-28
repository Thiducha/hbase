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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

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
 * HBASE_IT_MAIN_BOX
 * HBASE_IT_WILLDIE_BOX
 * HBASE_IT_WILLSURVIVE_BOX
 * HBASE_IT_LATE_BOX
 */

public abstract class AbstractIntegrationTestRecovery {
  public static final String MTTR_SMALL_TIME_KEY = "hbase-it.mttr.small.time";
  public static final long MTTR_SMALL_TIME_DEFAULT = 20000;

  public static final String MTTR_LARGE_TIME_KEY = "hbase-it.mttr.large.time";
  public static final long MTTR_LARGE_TIME_DEFAULT = 10000000;

  protected String mainBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_0");
  protected String willDieBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_1");
  protected String willSurviveBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_2");
  protected String lateBox = ClusterManager.getEnvNotNull("HBASE_IT_BOX_3");

  protected static final Log LOG
    = LogFactory.getLog(AbstractIntegrationTestRecovery.class);
  protected final String tableName = this.getClass().getName();
  protected static final String COLUMN_NAME = "f";
  protected final int regionCount;


  protected static IntegrationTestingUtility util;
  protected HBaseCluster dhc;
  protected ClusterManager hcm;

  AbstractIntegrationTestRecovery() {
    regionCount = 10;
  }

  AbstractIntegrationTestRecovery(int regCount) {
    regionCount = regCount;
  }


  @BeforeClass
  public static void setUp() throws Exception {
    Configuration c = HBaseClusterManager.createHBaseConfiguration();

    Assert.assertTrue(c.getBoolean("hbase.cluster.distributed", false));

    IntegrationTestingUtility.setUseDistributedCluster(c);
    util = new IntegrationTestingUtility(c);
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

  protected void genericStart() throws Exception {
    // Initialize an empty cluster. We will start all services where we want to start them.
    util.initializeCluster(0);
    dhc =  util.getHBaseClusterInterface();
    hcm = util.createClusterManager();

    // In case we stopped the previous test while is was not connected
    hcm.replug(willDieBox);

    hcm.checkAccessible(mainBox);
    hcm.checkAccessible(willDieBox);
    hcm.checkAccessible(willSurviveBox);
    hcm.checkAccessible(lateBox);

    // locally, you(re suppose to do the work yourself between the tests
    //  that's because you may have multiple java process on the main box.
    hcm.killAllServices(mainBox);
    hcm.killAllServices(willDieBox);
    hcm.killAllServices(willSurviveBox);
    hcm.killAllServices(lateBox);

    hcm.rmHDFSDataDir(mainBox);
    hcm.rmHDFSDataDir(willDieBox);
    hcm.rmHDFSDataDir(willSurviveBox);
    hcm.rmHDFSDataDir(lateBox);

    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    hcm.formatNameNode(mainBox); // synchronous

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    dhc.waitForDatanodesRegistered(4);


    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    // We want meta & root on the main server, so we start only one RS at the beginning

    while (!dhc.waitForActiveAndReadyMaster() ||
      util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 2) {
      Thread.sleep(200);
    }

    // No balance please
    util.getHBaseAdmin().setBalancerRunning(false, true);

    // We can now start the second master
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
    while (util.getHBaseAdmin().getClusterStatus().getServersSize() != 2) {
      Thread.sleep(200);
    }

    // Now we have 2 region servers and 4 datanodes.
  }


  public long getMttrSmallTime(){
    return hcm.getConf().getLong(MTTR_SMALL_TIME_KEY, MTTR_SMALL_TIME_DEFAULT);
  }

  public long getMttrLargeTime(){
    return hcm.getConf().getLong(MTTR_LARGE_TIME_KEY, MTTR_LARGE_TIME_DEFAULT);
  }


  /**
   * Kills all processes and delete the data dir. It's often better to not do that, as it allows
   * inspecting the cluster manually if something is strange.
   */
  protected void genericStop() throws IOException {
    hcm.killAllServices(mainBox);
    hcm.killAllServices(willDieBox);
    hcm.killAllServices(willSurviveBox);
    hcm.killAllServices(lateBox);

    hcm.rmHDFSDataDir(mainBox);
    hcm.rmHDFSDataDir(willDieBox);
    hcm.rmHDFSDataDir(willSurviveBox);
    hcm.rmHDFSDataDir(lateBox);
  }


  protected void moveToSecondRSSync() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    ServerName mainSN = dhc.getServerHoldingMeta();

    for (HRegionInfo hri : admin.getTableRegions(tableName.getBytes())) {
      if (dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
        admin.move(hri.getEncodedNameAsBytes(), null);
      }
    }

    // wait for the moves to be done
    waitForNoTransition();

    // Check that the regions have been moved
    int remaining = 0;
    for (HRegionInfo hri : admin.getTableRegions(tableName.getBytes())) {
      if (dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
        remaining++;
      }
    }

    assert remaining == 0 : "Some regions did not move! remaining=" + remaining;
  }

  @Test
  public void testKillRS() throws Exception {
    beforeStart();
    genericStart();

    HBaseAdmin admin = util.getHBaseAdmin();

    createTable();

    // now moving all the regions on the regionserver we're gonna kil
    moveToSecondRSSync();

    beforeKill();

    // Now killing
    final long startTime;
    final long failureDetectedTime;
    final long failureFixedTime;

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

      afterDetection();

      // Now, how long does it take to recover?
      boolean ok;
      ServerName mainSN = dhc.getServerHoldingMeta();
      do {
        waitForNoTransition();
        ok = true;
        for (HRegionInfo hri : admin.getTableRegions(tableName.getBytes())) {
          try {
            if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
              ok = false;
              break;
            }
          } catch (IOException ignored) {
            // It seems we can receive exceptions if the regionserver is dead...
            ok = false;
          }
        }
      } while (!ok);

      failureFixedTime = System.currentTimeMillis();

      afterRecovery();

    } finally

    {
      // If it was an unplug, we replug it now
      hcm.replug(willDieBox);
    }

    LOG.info("Detection took: " + (failureDetectedTime - startTime));
    LOG.info(("Failure fix took: " + (failureFixedTime - failureDetectedTime)));

    validate((failureDetectedTime - startTime), (failureFixedTime - failureDetectedTime));

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
