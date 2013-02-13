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
import org.apache.hadoop.hbase.DistributedHBaseCluster;
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
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

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
 */

public abstract class AbstractIntegrationTestRecovery {
  protected String mainBox = HBaseCluster.getEnvNotNull("HBASE_IT_MAIN_BOX");
  protected String willDieBox = HBaseCluster.getEnvNotNull("HBASE_IT_WILLDIE_BOX");
  protected String willSurviveBox = HBaseCluster.getEnvNotNull("HBASE_IT_WILLSURVIVE_BOX");
  protected String lateBox = HBaseCluster.getEnvNotNull("HBASE_IT_LATE_BOX");

  protected static final Log LOG
      = LogFactory.getLog(AbstractIntegrationTestRecovery.class);
  protected final String TABLE_NAME = this.getClass().getName();
  protected static final String COLUMN_NAME = "f";
  protected final int REGION_COUNT;


  protected static IntegrationTestingUtility util;
  protected DistributedHBaseCluster dhc;
  protected HBaseClusterManager hcm;

  AbstractIntegrationTestRecovery() {
    REGION_COUNT = 10;
  }

  AbstractIntegrationTestRecovery(int regCount) {
    REGION_COUNT = regCount;
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
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    RegionSplitter.SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(REGION_COUNT);

    LOG.info(String.format("Creating table %s with %d splits.", TABLE_NAME, REGION_COUNT));
    startTime = System.currentTimeMillis();
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(desc, splits);

    while (util.getHBaseAdmin().getClusterStatus().getRegionsCount() != REGION_COUNT + 2) {
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
    dhc = (DistributedHBaseCluster) util.getHBaseClusterInterface();
    hcm = (HBaseClusterManager) dhc.getClusterManager();

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

    hcm.rmDataDir(mainBox);
    hcm.rmDataDir(willDieBox);
    hcm.rmDataDir(willSurviveBox);
    hcm.rmDataDir(lateBox);

    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    hcm.formatNN(mainBox); // synchronous

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

    // Now we have 2 region servers and 3 datanodes.
  }

  /**
   * Kills all processes and delete the data dir. It's often better to not do that, as it allows
   * inspecting the cluster manually if something is strange.
   */
  private void genericStop() throws IOException {
    hcm.killAllServices(mainBox);
    hcm.killAllServices(willDieBox);
    hcm.killAllServices(willSurviveBox);
    hcm.killAllServices(lateBox);

    hcm.rmDataDir(mainBox);
    hcm.rmDataDir(willDieBox);
    hcm.rmDataDir(willSurviveBox);
    hcm.rmDataDir(lateBox);
  }


  protected void moveToRS(String tableName, ServerName destSN) throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();
    List<HRegionInfo> regs = admin.getTableRegions(tableName.getBytes());

    int toMove = 0;
    for (HRegionInfo hri : regs) {
      if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(destSN)) {
        admin.move(hri.getEncodedNameAsBytes(), destSN.getVersionedBytes());
        toMove++;
      }
    }

    // wait for the moves to be done
    waitForNoTransition();

    // Check that they have been moved
    int moved = 0;
    for (HRegionInfo hri : regs) {
      if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(destSN)) {
        moved++;
      }
    }

    System.out.println("toMove=" + toMove + ", moved=" + moved);
    // todo: it differs sometimes, it should not!


    admin.close();
  }

  @Test
  public void testKillRS() throws Exception {
    beforeStart();
    genericStart();

    HBaseAdmin admin = util.getHBaseAdmin();

    createTable();

    // now moving all the regions on the regionserver we're gonna kill
    List<HRegionInfo> regs = admin.getTableRegions(TABLE_NAME.getBytes());
    ServerName mainSN = dhc.getServerHoldingMeta();

    // todo: find a less horrible way to get this servername
    ServerName otherSN = null;
    for (HRegionInfo hri : regs) {
      if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
        otherSN = dhc.getServerHoldingRegion(hri.getRegionName());
        break;
      }
    }
    Assert.assertNotNull(otherSN);
    Assert.assertNotEquals(mainSN, otherSN);

    moveToRS(TABLE_NAME, otherSN);

    beforeKill();

    // Now killing
    final long startTime;
    final long failureDetectedTime;
    final long failureFixedTime;

    kill(willDieBox);

    // Check that the RS is really killed. We do that by trying to connect to the server with
    //  a minimum connect timeout. If it succeeds, it means it's still there...
    try {
      Socket socket = new Socket();
      InetSocketAddress dest = new InetSocketAddress(
          willDieBox, util.getConfiguration().getInt("hbase.regionserver.port", 60020));
      boolean stillThere = true;
      while (stillThere) {
        try {
          Thread.sleep(100);
          socket.connect(dest, 400);
        } catch (IOException ignored) {
          stillThere = false;
        }
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
      do {
        waitForNoTransition();
        ok = true;
        for (HRegionInfo hri : regs) {
          try {
            if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)) {
              ok = false;
            }
          } catch (IOException e) {
            // It seems we can receive exceptions if the regionserver is dead...
            ok = false;
          }
        }
      } while (!ok);

      failureFixedTime = System.currentTimeMillis();

      afterRecovery();

    } finally {
      // If it was an unplug, we replug it now
      hcm.replug(willDieBox);
    }
    LOG.info("Detection took: " + (failureDetectedTime - startTime));
    LOG.info(("Failure fix took: " + (failureFixedTime - failureDetectedTime)));
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
   * @param willDieBox the box to kill
   * @throws Exception
   */
  protected abstract void kill(String willDieBox) throws Exception;

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
}
