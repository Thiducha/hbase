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
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTable {

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

  protected static final int REGION_COUNT = 100;

  protected String masterBox = "127.0.0.1";
  protected String willDieBox = "azwaw";


  protected static final IntegrationTestingUtility util = new IntegrationTestingUtility();
  protected DistributedHBaseCluster dhc;
  protected HBaseClusterManager hcm;


  @Before
  public void setUp() throws Exception {
    // We need a replication factor of two to use only 3 nodes.
    util.getConfiguration().setInt("dfs.replication", 2);

    // ideally we would start only one node
    util.initializeCluster(2);
    LOG.info("Cluster initialized");

    HBaseAdmin admin = util.getHBaseAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      LOG.info(String.format("Deleting existing table %s.", TABLE_NAME));
      if (admin.isTableEnabled(TABLE_NAME)) admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
      LOG.info(String.format("Existing table %s deleted.", TABLE_NAME));
    }
    dhc = (DistributedHBaseCluster) util.getHBaseClusterInterface();
    hcm = (HBaseClusterManager) dhc.getClusterManager();
    LOG.info("Cluster ready");
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

  @Test
  public void testUnplugRS() throws Exception {
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    // ideally, we should wait here. If we start the DN too early,the blocks will be written
    // to another datanode. It's not critical in this test as the table is empty.

    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, willDieBox);
    // Here it would be better to wait as well: if not the regions will be created on another regionserver

    createTable();

    // Again, we need to wait to have the regions fully available.

    HBaseAdmin admin = util.getHBaseAdmin();

    while (admin.getClusterStatus().getRegionsInTransition().size() >0){
      Thread.sleep(1000);
    }

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
