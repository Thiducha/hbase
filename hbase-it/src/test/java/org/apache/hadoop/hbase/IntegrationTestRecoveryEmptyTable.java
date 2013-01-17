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

package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * An integration test to measure the time needed to recover when we loose a regionserver.
 * Makes sense only with a distributed cluster.
 *
 *
 * We try to use default config as much as possible.
 * So:
 * We need 4 boxes, because we have a replication factor of 3. If we kill a box, we still need
 *  3 live boxes to make keep happy.
 * We have a single ZK, on the master box. This partly minimizes the ZK cost. Partly, because
 *  this box is also a DN, so ZK with fight with the DN for i/o resources.
 *
 * Scenario:
 *  - start 3 boxes
 *  - create a table
 *  - have all regions of this table on a single one
 *  - start another box
 *  - kill the box with all regions
 *  - measure time.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTable {

  protected String mainBox = "127.0.0.1";
  protected String willDieBox = "azwaw";
  protected String willSurviveBox = "marc";
  protected String lateBox = "aa";




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




  protected static final IntegrationTestingUtility util = new IntegrationTestingUtility();
  protected DistributedHBaseCluster dhc;
  protected HBaseClusterManager hcm;


  @Before
  public void setUp() throws Exception {
    // ideally we would start only one node
    util.initializeCluster(3);
    LOG.info("Cluster initialized");

    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      LOG.info(String.format("Deleting existing table %s.", TABLE_NAME));
      if (admin.isTableEnabled(TABLE_NAME)) admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
      LOG.info(String.format("Existing table %s deleted.", TABLE_NAME));
    }

  }




  @After
  public void tearDown() throws IOException {
    LOG.info("Cleaning up after test.");
    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      if (admin.isTableEnabled(TABLE_NAME)) admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
    }
    LOG.info("Restoring cluster.");
    util.restoreCluster();
    LOG.info("Cluster restored.");
  }

  private void createTable() throws IOException {
    long startTime, endTime;
    HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    RegionSplitter.SplitAlgorithm algo = new RegionSplitter.HexStringSplit();
    byte[][] splits = algo.split(REGION_COUNT);

    LOG.info(String.format("Creating table %s with %d splits.", TABLE_NAME, REGION_COUNT));
    startTime = System.currentTimeMillis();
    HBaseAdmin admin = util.getHBaseAdmin();
    admin.createTable(desc, splits);
    endTime = System.currentTimeMillis();
    LOG.info(String.format("Pre-split table created successfully in %dms.",
        (endTime - startTime)));
  }

  private void waitForNoTransition() throws Exception {
    HBaseAdmin admin = util.getHBaseAdmin();

    while (admin.getClusterStatus().getRegionsInTransition().size() >0){
      Thread.sleep(1000);
    }
  }

  private void genericStart() throws Exception {
    // I want to automate everything, man. So I start the cluster here.

    util.initializeCluster(0);

    dhc = (DistributedHBaseCluster) util.getHBaseClusterInterface();
    hcm = (HBaseClusterManager) dhc.getClusterManager();
    LOG.info("Cluster ready");

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);

    Thread.sleep(40000);// Need to wait

    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, mainBox);

    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);
    Thread.sleep(40000);// Need to wait

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    Thread.sleep(40000);// Need to wait
  }

  private void testMachines() throws Exception{
    hcm.checkAccessible(mainBox);
  }


  @Test
  public void testUnplugRS() throws Exception {

    genericStart();

    createTable();

    //moveTable();

    HBaseAdmin admin = util.getHBaseAdmin();
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
