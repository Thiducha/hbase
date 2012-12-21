/**
 *
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

package org.apache.hadoop.hbase.regionserver;

import com.google.protobuf.ServiceException;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Tests on the region server, without the master.
 */
@Category(MediumTests.class)
public class TestRegionServerNoMaster {

  private static final int NB_SERVERS = 1;
  private static HTable table;
  private static final byte[] row = "ee".getBytes();

  private static HRegionInfo hri;

  private static byte[] regionName;
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();


  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
    final byte[] tableName = Bytes.toBytes(TestRegionServerNoMaster.class.getName());

    // Create table then get the single region for our new table.
    table = HTU.createTable(tableName, HConstants.CATALOG_FAMILY);
    Put p = new Put(row);
    p.add(HConstants.CATALOG_FAMILY, row, row);
    table.put(p);

    hri = table.getRegionLocation(row, false).getRegionInfo();
    regionName = hri.getRegionName();

    // No master
    HTU.getHBaseCluster().getMaster().stopMaster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    table.close();
    HTU.shutdownMiniCluster();
  }


  private static HRegionServer getRS() {
    return HTU.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
  }

  @After
  public void after() throws Exception {
    // Clean the state if the test failed before cleaning the znode
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hri);
  }

  /**
   * Reopen the region. Reused in multiple tests as we always leave the region open after a test.
   */
  private void reopenRegion() throws Exception {
    // We reopen. We need a ZK node here, as a open is always triggered by a master.
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0);
    AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
    Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);
    Assert.assertTrue(responseOpen.getOpeningState(0).
        equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED));


    checkRegionIsOpened();
  }

  private void checkRegionIsOpened() throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(getRS().getRegion(regionName).isAvailable());

    Assert.assertTrue(
        ZKAssign.deleteOpenedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName()));
  }


  private void checkRegionIsClosed() throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    try {
      Assert.assertFalse(getRS().getRegion(regionName).isAvailable());
    } catch (NotServingRegionException expected) {
      // That's how it work: if the region is closed we have an exception.
    }

    // We don't delete the znode here, because there is not always a znode.
  }


  /**
   * Close the region without using ZK
   */
  private void closeNoZK() throws Exception {
    // no transition in ZK
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, false);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    // now waiting & checking. After a while, the transition should be done and the region closed
    checkRegionIsClosed();
  }


  @Test(timeout = 10000)
  public void testCloseByRegionServer() throws Exception {
    closeNoZK();
    reopenRegion();
  }

  @Test(timeout = 10000)
  public void testCloseByMasterWithoutZNode() throws Exception {

    // Transition in ZK on. This should fail, as there is no znode
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    // now waiting. After a while, the transition should be done
    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    // the region is still available, the close got rejected at the end
    Assert.assertTrue("The close should have failed", getRS().getRegion(regionName).isAvailable());
  }

  @Test(timeout = 10000)
  public void testOpenCloseByMasterWithZNode() throws Exception {

    ZKAssign.createNodeClosing(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());

    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    checkRegionIsClosed();

    ZKAssign.deleteClosedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName());

    reopenRegion();
  }

  /**
   * Test that we can send multiple openRegion to the region server.
   * This is used when:
   * - there is a SocketTimeout: in this case, the master does not know if the region server
   * received the request before the timeout.
   * - We have a socket error during the operation: same stuff: we don't know
   * - a master failover: if we find a znode in thz M_ZK_REGION_OFFLINE, we don't know if
   * the region server has received the query or not. Only solution to be efficient: re-ask
   * immediately.
   */
  @Test(timeout = 10000)
  public void testMultipleOpen() throws Exception {

    // We close
    closeNoZK();
    checkRegionIsClosed();

    // We reopen. We need a ZK node here, as a open is always triggered by a master.
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());

    // We're sending multiple requests in a row. The regionserver must handle this nicely.
    for (int i = 0; i < 10; i++) {
      AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0);
      AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
      Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);

      AdminProtos.OpenRegionResponse.RegionOpeningState ors = responseOpen.getOpeningState(0);
      Assert.assertTrue("request " + i + " failed",
          ors.equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED) ||
              ors.equals(AdminProtos.OpenRegionResponse.RegionOpeningState.ALREADY_OPENED)
      );
    }

    checkRegionIsOpened();
  }

  @Test(timeout = 10000)
  public void testMultipleCloseFromMaster() throws Exception {

    // As opening, we must support multiple requests on the same region
    ZKAssign.createNodeClosing(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    for (int i = 0; i < 10; i++) {
      AdminProtos.CloseRegionRequest crr =
          RequestConverter.buildCloseRegionRequest(regionName, 0, null, true);
      AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
      Assert.assertTrue("request " + i + " failed",
          responseClose.getClosed() || responseClose.hasClosed());
    }

    checkRegionIsClosed();

    ZKAssign.deleteClosedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName());

    reopenRegion();
  }

  /**
   * Test that if we do a close while opening it stops the opening.
   */
  @Test(timeout = 10000)
  public void testCancelOpeningWithoutZK() throws Exception {
    // We close
    closeNoZK();
    checkRegionIsClosed();

    // Let do the initial steps, without having a handler
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    getRS().getRegionsInTransitionInRS().put(hri.getEncodedNameAsBytes(), Boolean.TRUE);

    // That's a close without ZK.
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, false);
    try {
      getRS().closeRegion(null, crr);
      Assert.assertTrue(false);
    }catch ( ServiceException expected) {
    }

    // The state in RIT should have changed to close
    Assert.assertEquals(Boolean.FALSE, getRS().getRegionsInTransitionInRS().get(
        hri.getEncodedNameAsBytes()));

    // Let's start the open handler
    HTableDescriptor htd = getRS().tableDescriptors.get(hri.getTableName());
    getRS().service.submit(new OpenRegionHandler(getRS(), getRS(), hri, htd, 0));

    // The open handler should have removed the region from RIT but kept the region closed
    checkRegionIsClosed();

    // Today, this leaves a znode, we need to delete it 'manually'
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hri);

    reopenRegion();
  }
}
