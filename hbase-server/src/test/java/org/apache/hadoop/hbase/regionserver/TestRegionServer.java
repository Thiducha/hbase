package org.apache.hadoop.hbase.regionserver;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests on the region server, without the master.
 */
public class TestRegionServer {
  private static final Log LOG =
      LogFactory.getLog(TestRegionServer.class);
  private static final int NB_SERVERS = 1;
  private static HTable table;
  private static final byte[] row = "ee".getBytes();

  private static HRegionInfo hri;

  private static byte[] regionName;
  private static final HBaseTestingUtility TESTING_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void before() throws Exception {
    TESTING_UTIL.startMiniCluster(NB_SERVERS);
    final byte[] tableName = Bytes.toBytes("testMultipleOpen");

    // Create table then get the single region for our new table.
    table = TESTING_UTIL.createTable(tableName, HConstants.CATALOG_FAMILY);
    Put p = new Put(row);
    p.add(HConstants.CATALOG_FAMILY, row, row);
    table.put(p);

    hri = table.getRegionLocation(row).getRegionInfo();
    regionName = hri.getRegionName();

    // No master
    TESTING_UTIL.getHBaseCluster().getMaster().stopMaster();
  }

  @AfterClass
  public static void after() throws Exception {
    table.close();
    TESTING_UTIL.shutdownMiniCluster();
  }


  private static HRegionServer getRS() {
    return TESTING_UTIL.getHBaseCluster().getLiveRegionServerThreads().get(0).getRegionServer();
  }


  /**
   * Reopen the region. Reused is multiple tests as we always leave the region open after a test.
   *
   * @throws Exception
   */
  private void reopenRegion() throws Exception {
    // We reopen. We need a ZK node here, as a open is always triggered by a master.
    ZKAssign.createNodeOffline(TESTING_UTIL.getZooKeeperWatcher(), hri, getRS().getServerName());
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0);
    AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
    Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);
    Assert.assertTrue(responseOpen.getOpeningState(0).
        equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED));


    checkOpened();
  }

  private void checkOpened() throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) Thread.sleep(1);

    Assert.assertTrue(getRS().getRegion(regionName).isAvailable());

    Assert.assertTrue(
        ZKAssign.deleteOpenedNode(TESTING_UTIL.getZooKeeperWatcher(), hri.getEncodedName()));
  }


  private void checkRegionIsClosed() {
    try {
      getRS().getRegion(regionName);
      Assert.assertTrue("close unsuccessful", false);
    } catch (NotServingRegionException expected) {
      // That's how it work...
    }
  }


  private void closeNoZK() throws Exception {
    // no transition in ZK
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, false);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    // now waiting. After a while, the transition should be done and the region closed
    while (!getRS().getRegionsInTransitionInRS().isEmpty()) Thread.sleep(1);

    checkRegionIsClosed();
  }

  @Test(timeout = 10000)
  public void testCloseByRegionServer() throws Exception {
    closeNoZK();
    reopenRegion();
  }

  // @Test (timeout = 10000)     // fails, the region is closed, it should not??
  public void testCloseByMasterWithoutZNode() throws Exception {

    // Transition in ZK on. This should fail, as there is no znode
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    // now waiting. After a while, the transition should be done and the region closed
    while (!getRS().getRegionsInTransitionInRS().isEmpty()) Thread.sleep(1);

    Assert.assertTrue("The close should have failed", getRS().getRegion(regionName).isAvailable());
  }

  @Test(timeout = 10000)
  public void testOpenCloseByMasterWithZNode() throws Exception {

    ZKAssign.createNodeClosing(TESTING_UTIL.getZooKeeperWatcher(), hri, getRS().getServerName());
    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(regionName, true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) Thread.sleep(1);
    checkRegionIsClosed();
    ZKAssign.deleteClosedNode(TESTING_UTIL.getZooKeeperWatcher(), hri.getEncodedName());

    reopenRegion();
  }

  /**
   * Test that we can send multiple openRegion to the region server.
   * This is used when:
   * - there is a SocketTimeout: in this case, the master does not know if the region server
   * received the request before the timeout.
   * - A socket error during the operation: same stuff: we don't know
   * - a master failover: if we find a znode in thz M_ZK_REGION_OFFLINE, we don't know if
   * the region server has received the query on not. Only solution to be efficient: re-ask
   * immediately.
   */
  //@Test(timeout = 10000)  // Fails, should not
  public void testMultipleOpen() throws Exception {
    closeNoZK();

    // We reopen. We need a ZK node here, as a open is always triggered by a master.
    ZKAssign.createNodeOffline(TESTING_UTIL.getZooKeeperWatcher(), hri, getRS().getServerName());

    // We're sending multiple requests in a row. The regionserver must handle this nicely.
    for (int i = 0; i < 10; i++) {
      AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0);
      AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
      Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);

      AdminProtos.OpenRegionResponse.RegionOpeningState ors = responseOpen.getOpeningState(0);
      Assert.assertTrue(
          ors.equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED) ||
              ors.equals(AdminProtos.OpenRegionResponse.RegionOpeningState.ALREADY_OPENED) ||
              ors.equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED)
      );
    }

    checkOpened();
  }
}
