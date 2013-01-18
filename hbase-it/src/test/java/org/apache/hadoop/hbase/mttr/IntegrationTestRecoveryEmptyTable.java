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
import org.apache.hadoop.hbase.HBaseClusterManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * An integration test to measure the time needed to recover when we loose a regionserver.
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
 * - kill the box with all regions
 * - measure time.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTable {

  protected String mainBox = "127.0.0.1";
  protected String willDieBox = "192.168.1.13";
  protected String willSurviveBox = "192.168.1.12";
  protected String lateBox = "192.168.1.15";


  private static final String CLASS_NAME
      = IntegrationTestRecoveryEmptyTable.class.getSimpleName();

  protected static final Log LOG
      = LogFactory.getLog(IntegrationTestRecoveryEmptyTable.class);
  protected static final String TABLE_NAME = CLASS_NAME;
  protected static final String COLUMN_NAME = "f";
  protected static final String REGION_COUNT_KEY
      = String.format("hbase.%s.regions", CLASS_NAME);
  protected static final String REGIONSERVER_COUNT_KEY
      = String.format("hbase.%s.regionServers", CLASS_NAME);
  protected static final String TIMEOUT_MINUTES_KEY
      = String.format("hbase.%s.timeoutMinutes", CLASS_NAME);

  protected static final int REGION_COUNT = 10;


  protected static IntegrationTestingUtility util;
  protected DistributedHBaseCluster dhc;
  protected HBaseClusterManager hcm;


  private static URL getConfFile(String file) throws MalformedURLException {
    File case1 = new File(".." + File.separator + "hbase-it" + File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + file);
    File case2 = new File("hbase-it" + File.separator + "src" + File.separator + "test" + File.separator + "resources" + File.separator + file);

    if (case1.exists() && case1.canRead()) {
      return case1.toURI().toURL();
    }

    if (case2.exists() && case2.canRead()) {
      return case2.toURI().toURL();
    }

    throw new RuntimeException("Can't find " + file + " tried " + case1.getAbsolutePath() + " and " + case2.getAbsolutePath());
  }

  @BeforeClass
  public static void setUp() throws Exception {
    // ideally we would start only one node
    Configuration c = new Configuration();
    c.clear();

    c.addResource(getConfFile("it-core-site.xml"));
    c.addResource(getConfFile("it-hdfs-site.xml"));
    c.addResource(getConfFile("it-hbase-site.xml"));

    Assert.assertTrue(c.getBoolean("hbase.cluster.distributed", false));

    IntegrationTestingUtility.setUseDistributedCluster(c);
    util = new IntegrationTestingUtility(c);
  }


  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    LOG.info("Restoring cluster.");
    LOG.info("Cluster restored.");
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

    while (util.getHBaseAdmin().getClusterStatus().getRegionsCount() != REGION_COUNT + 2 || !util.getHBaseAdmin().getClusterStatus().getRegionsInTransition().isEmpty()) {
      Thread.sleep(1000);
    }

    endTime = System.currentTimeMillis();

    LOG.info(String.format("Pre-split table created successfully in %dms.",
        (endTime - startTime)));
  }

  private void waitForNoTransition() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();

    while (admin.getClusterStatus().getRegionsInTransition().size() > 0) {
      Thread.sleep(1000);
    }
  }

  private void genericStart() throws Exception {
    // I want to automate everything, man. So I start the cluster here.

    util.initializeCluster(0);

    dhc = (DistributedHBaseCluster) util.getHBaseClusterInterface();
    hcm = (HBaseClusterManager) dhc.getClusterManager();

    hcm.checkAccessible(mainBox);
    hcm.checkAccessible(willDieBox);
    hcm.checkAccessible(willSurviveBox);
    hcm.checkAccessible(lateBox);

    // locally, you(re suppose to do the work yourself between the tests
    //  that's because you may have multiple java process on the main box.
    hcm.killJavas(mainBox);
    hcm.killJavas(willDieBox);
    hcm.killJavas(willSurviveBox);
    hcm.killJavas(lateBox);

    hcm.rmDataDir(mainBox);
    hcm.rmDataDir(willDieBox);
    hcm.rmDataDir(willSurviveBox);
    hcm.rmDataDir(lateBox);

    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    hcm.formatNN(mainBox); // synchronous

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);

    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);

    Thread.sleep(10000);
    dhc.waitForDatanodesRegistered(3);


    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    // We want meta & root on the main server.

    Thread.sleep(10000);
    while (util.getHBaseAdmin().getClusterStatus().getServersSize() == 0) {
      Thread.sleep(1000);
    }

    boolean masterOk = false;
    while (!masterOk) {
      try {
        masterOk = util.getHBaseAdmin().isMasterRunning() && util.getHBaseAdmin().getClusterStatus().getRegionsCount()==2;
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }


    // No balance here.
    util.getHBaseAdmin().setBalancerRunning(false, true);

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
    while (util.getHBaseAdmin().getClusterStatus().getServersSize() != 2) {
      Thread.sleep(1000);
    }


    // Now we have 2 region servers and 3 datanodes.
  }

  private void testMachines() throws Exception {
    hcm.checkAccessible(mainBox);
  }


  @Test
  public void testUnplugRS() throws Exception {

    genericStart();

    HBaseAdmin admin = util.getHBaseAdmin();

    createTable();

    // now moving all the regions on the regionserver we're gonna kill
    List<HRegionInfo> regs = admin.getTableRegions(TABLE_NAME.getBytes());
    ServerName mainSN = dhc.getServerHoldingMeta();

    // todo: find a less horrible way to get this servername
    ServerName otherSN = null;
    for (HRegionInfo hri:regs){
      if (!dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)){
        otherSN = dhc.getServerHoldingRegion(hri.getRegionName());
        break;
      }
    }
    Assert.assertNotNull(otherSN);
    Assert.assertNotEquals(mainSN, otherSN);

    for (HRegionInfo hri:regs){
      if (dhc.getServerHoldingRegion(hri.getRegionName()).equals(mainSN)){
        admin.move(hri.getEncodedNameAsBytes(), otherSN.getVersionedBytes() );
      }
    }

    // wait for the moves to be done
    while (util.getHBaseAdmin().getClusterStatus().getRegionsCount() != REGION_COUNT + 2 ||
        !util.getHBaseAdmin().getClusterStatus().getRegionsInTransition().isEmpty()) {
      Thread.sleep(1000);
    }

    // Adding a new datanode
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    dhc.waitForDatanodesRegistered(4);


    // Now killing

    final long startTime = System.currentTimeMillis();
    final long failureDetectedTime;
    final long failureFixedTime;
    hcm.unplug(willDieBox);
    try {
      while (admin.getClusterStatus().getRegionsInTransition().size() == 0) {
        Thread.sleep(1000);
      }
      failureDetectedTime = System.currentTimeMillis();
      while (admin.getClusterStatus().getRegionsInTransition().size() != 0) {
        Thread.sleep(1000);
      }
      failureFixedTime = System.currentTimeMillis();

    } finally {
      hcm.replug(willDieBox);
    }
    System.out.println("Detection took: " + (failureDetectedTime - startTime));
    System.out.println("Failure fix took: " + (failureFixedTime - failureDetectedTime));
  }
}
