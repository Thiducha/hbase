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
package org.apache.hadoop.hbase.catalog;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.Progressable;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test {@link CatalogTracker}
 */
@Category(MediumTests.class)
public class TestCatalogTracker {
  private static final Log LOG = LogFactory.getLog(TestCatalogTracker.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final ServerName SN =
    new ServerName("example.org", 1234, System.currentTimeMillis());
  private ZooKeeperWatcher watcher;
  private Abortable abortable;

  @BeforeClass public static void beforeClass() throws Exception {
    // Set this down so tests run quicker
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    UTIL.startMiniZKCluster();
  }

  @AfterClass public static void afterClass() throws IOException {
    UTIL.getZkCluster().shutdown();
  }

  @Before public void before() throws IOException {
    this.abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }

      @Override
      public boolean isAborted()  {
        return false;
      }
    };
    this.watcher = new ZooKeeperWatcher(UTIL.getConfiguration(),
      this.getClass().getSimpleName(), this.abortable, true);
  }

  @After public void after() {
    this.watcher.close();
  }

  private CatalogTracker constructAndStartCatalogTracker(final HConnection c)
  throws IOException, InterruptedException {
    CatalogTracker ct = new CatalogTracker(this.watcher, UTIL.getConfiguration(),
      c, this.abortable);
    ct.start();
    return ct;
  }

  /**
   * Test that we get notification if .META. moves.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test public void testThatIfMETAMovesWeAreNotified()
  throws IOException, InterruptedException, KeeperException {
    HConnection connection = Mockito.mock(HConnection.class);
    constructAndStartCatalogTracker(connection);
    try {
      RootRegionTracker.setRootLocation(this.watcher,
        new ServerName("example.com", 1234, System.currentTimeMillis()));
    } finally {
      // Clean out root location or later tests will be confused... they presume
      // start fresh in zk.
      RootRegionTracker.deleteRootLocation(this.watcher);
    }
  }

  /**
   * Test interruptable while blocking wait on root and meta.
   * @throws IOException
   * @throws ServiceException
   * @throws InterruptedException
   */
  @Test public void testInterruptWaitOnMetaAndRoot()
  throws IOException, InterruptedException, ServiceException {
    final ClientProtocol client = Mockito.mock(ClientProtocol.class);
    HConnection connection = mockConnection(null, client);
    try {
      Mockito.when(client.get((RpcController)Mockito.any(), (GetRequest)Mockito.any())).
      thenReturn(GetResponse.newBuilder().build());
      final CatalogTracker ct = constructAndStartCatalogTracker(connection);
      ServerName hsa = ct.getRootLocation();
      Assert.assertNull(hsa);
      ServerName meta = ct.getMetaLocation();
      Assert.assertNull(meta);
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            ct.waitForMeta();
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted", e);
          }
        }
      };
      t.start();
      while (!t.isAlive())
        Threads.sleep(1);
      Threads.sleep(1);
      assertTrue(t.isAlive());
      ct.stop();
      // Join the thread... should exit shortly.
      t.join();
    } finally {
      HConnectionManager.deleteConnection(UTIL.getConfiguration());
    }
  }

  /**
   * Test for HBASE-4288.  Throw an IOE when trying to verify meta region and
   * prove it doesn't cause master shutdown.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-4288">HBASE-4288</a>
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test
  public void testServerNotRunningIOException()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    // Mock an Admin and a Client.
    final AdminProtocol admin = Mockito.mock(AdminProtocol.class);
    final ClientProtocol client = Mockito.mock(ClientProtocol.class);
    HConnection connection = mockConnection(admin, client);
    try {
      // If a 'getRegionInfo' is called on mocked AdminProtocol, throw IOE
      // the first time.  'Succeed' the second time we are called.
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(new HRegionInfo(Bytes.toBytes("test"))));
      Mockito.when(admin.getRegionInfo((RpcController)Mockito.any(),
        (GetRegionInfoRequest)Mockito.any())).thenThrow(
          new ServiceException(new IOException("Server not running, aborting"))).
        thenReturn(builder.build());

      // After we encounter the above 'Server not running', we should catch the
      // IOE and go into retrying for the meta mode.  We'll do gets on -ROOT- to
      // get new meta location.  Return something so this 'get' succeeds
      // (here we mock up getRegionServerWithRetries, the wrapper around
      // the actual get).

      // TODO: Refactor.  This method has been moved out of HConnection.
      // It works for now but has been deprecated.
      Mockito.when(connection.getRegionServerWithRetries((ServerCallable<Result>)Mockito.any())).
        thenReturn(getMetaTableRowResult());

      Mockito.when(client.get((RpcController)Mockito.any(), (GetRequest)Mockito.any())).
        thenReturn(GetResponse.newBuilder().build());
      // Now start up the catalogtracker with our doctored Connection.
      final CatalogTracker ct = constructAndStartCatalogTracker(connection);
      try {
        // Set a location for root and meta.
        RootRegionTracker.setRootLocation(this.watcher, SN);
        ct.setMetaLocation(SN);
        // Call the method that HBASE-4288 calls.  It will try and verify the
        // meta location and will fail on first attempt then go into a long wait.
        // So, do this in a thread and then reset meta location to break it out
        // of its wait after a bit of time.
        final AtomicBoolean metaSet = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
          @Override
          public void run() {
            try {
              latch.countDown();
              metaSet.set(ct.waitForMeta(100000) !=  null);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
        t.start();
        latch.await();
        Threads.sleep(1);
        // Now reset the meta as though it were redeployed.
        ct.setMetaLocation(SN);
        t.join();
        Assert.assertTrue(metaSet.get());
      } finally {
        // Clean out root and meta locations or later tests will be confused...
        // they presume start fresh in zk.
        ct.resetMetaLocation();
        RootRegionTracker.deleteRootLocation(this.watcher);
      }
    } finally {
      // Clear out our doctored connection or could mess up subsequent tests.
      HConnectionManager.deleteConnection(UTIL.getConfiguration());
    }
  }

  private void testVerifyMetaRegionLocationWithException(Exception ex)
  throws IOException, InterruptedException, KeeperException, ServiceException {
    // Mock an ClientProtocol.
    final ClientProtocol implementation = Mockito.mock(ClientProtocol.class);
    HConnection connection = mockConnection(null, implementation);
    try {
      // If a 'get' is called on mocked interface, throw connection refused.
      Mockito.when(implementation.get((RpcController) Mockito.any(), (GetRequest) Mockito.any())).
        thenThrow(new ServiceException(ex));
      // Now start up the catalogtracker with our doctored Connection.
      final CatalogTracker ct = constructAndStartCatalogTracker(connection);
      try {
        RootRegionTracker.setRootLocation(this.watcher, SN);
        long timeout = UTIL.getConfiguration().
          getLong("hbase.catalog.verification.timeout", 1000);
        Assert.assertFalse(ct.verifyMetaRegionLocation(timeout));
      } finally {
        // Clean out root location or later tests will be confused... they
        // presume start fresh in zk.
        RootRegionTracker.deleteRootLocation(this.watcher);
      }
    } finally {
      // Clear out our doctored connection or could mess up subsequent tests.
      HConnectionManager.deleteConnection(UTIL.getConfiguration());
    }
  }

  /**
   * Test we survive a connection refused {@link ConnectException}
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testGetMetaServerConnectionFails()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ConnectException("Connection refused"));
  }

  /**
   * Test that verifyMetaRegionLocation properly handles getting a
   * ServerNotRunningException. See HBASE-4470.
   * Note this doesn't check the exact exception thrown in the
   * HBASE-4470 as there it is thrown from getHConnection() and
   * here it is thrown from get() -- but those are both called
   * from the same function anyway, and this way is less invasive than
   * throwing from getHConnection would be.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testVerifyMetaRegionServerNotRunning()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ServerNotRunningYetException("mock"));
  }

  /**
   * Test get of root region fails properly if nothing to connect to.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testVerifyRootRegionLocationFails()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    HConnection connection = Mockito.mock(HConnection.class);
    ServiceException connectException =
      new ServiceException(new ConnectException("Connection refused"));
    final AdminProtocol implementation =
      Mockito.mock(AdminProtocol.class);
    Mockito.when(implementation.getRegionInfo((RpcController)Mockito.any(),
      (GetRegionInfoRequest)Mockito.any())).thenThrow(connectException);
    Mockito.when(connection.getAdmin(Mockito.anyString(),
      Mockito.anyInt(), Mockito.anyBoolean())).
      thenReturn(implementation);
    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    try {
      RootRegionTracker.setRootLocation(this.watcher,
        new ServerName("example.com", 1234, System.currentTimeMillis()));
      Assert.assertFalse(ct.verifyRootRegionLocation(100));
    } finally {
      // Clean out root location or later tests will be confused... they presume
      // start fresh in zk.
      RootRegionTracker.deleteRootLocation(this.watcher);
    }
  }

  @Test (expected = NotAllMetaRegionsOnlineException.class)
  public void testTimeoutWaitForRoot()
  throws IOException, InterruptedException {
    HConnection connection = Mockito.mock(HConnection.class);
    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    ct.waitForRoot(100);
  }

  @Test (expected = RetriesExhaustedException.class)
  public void testTimeoutWaitForMeta()
  throws IOException, InterruptedException {
    HConnection connection =
      HConnectionTestingUtility.getMockedConnection(UTIL.getConfiguration());
    try {
      final CatalogTracker ct = constructAndStartCatalogTracker(connection);
      ct.waitForMeta(100);
    } finally {
      HConnectionManager.deleteConnection(UTIL.getConfiguration());
    }
  }

  /**
   * Test waiting on root w/ no timeout specified.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test public void testNoTimeoutWaitForRoot()
  throws IOException, InterruptedException, KeeperException {
    HConnection connection = Mockito.mock(HConnection.class);
    final CatalogTracker ct = constructAndStartCatalogTracker(connection);
    ServerName hsa = ct.getRootLocation();
    Assert.assertNull(hsa);

    // Now test waiting on root location getting set.
    Thread t = new WaitOnMetaThread(ct);
    startWaitAliveThenWaitItLives(t, 1);
    // Set a root location.
    hsa = setRootLocation();
    // Join the thread... should exit shortly.
    t.join();
    // Now root is available.
    Assert.assertTrue(ct.getRootLocation().equals(hsa));
  }

  private ServerName setRootLocation() throws KeeperException {
    RootRegionTracker.setRootLocation(this.watcher, SN);
    return SN;
  }

  /**
   * Test waiting on meta w/ no timeout specified.
   * @throws Exception
   */
  @Ignore // Can't make it work reliably on all platforms; mockito gets confused
  // Throwing: org.mockito.exceptions.misusing.WrongTypeOfReturnValue:
  // Result cannot be returned by locateRegion()
  // If you plug locateRegion, it then throws for incCounter, and if you plug
  // that ... and so one.
  @Test public void testNoTimeoutWaitForMeta()
  throws Exception {
    // Mock an HConnection and a AdminProtocol implementation.  Have the
    // HConnection return the HRI.  Have the HRI return a few mocked up responses
    // to make our test work.
    // Mock an AdminProtocol.
    final AdminProtocol implementation = Mockito.mock(AdminProtocol.class);
    HConnection connection = mockConnection(implementation, null);
    try {
      // Now the ct is up... set into the mocks some answers that make it look
      // like things have been getting assigned. Make it so we'll return a
      // location (no matter what the Get is). Same for getHRegionInfo -- always
      // just return the meta region.
      final Result result = getMetaTableRowResult();

      // TODO: Refactor.  This method has been moved out of HConnection.
      // It works for now but has been deprecated.
      Mockito.when(connection.getRegionServerWithRetries((ServerCallable<Result>)Mockito.any())).
        thenReturn(result);
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(HRegionInfo.FIRST_META_REGIONINFO));
      Mockito.when(implementation.getRegionInfo((RpcController)Mockito.any(),
        (GetRegionInfoRequest)Mockito.any())).thenReturn(builder.build());
      final CatalogTracker ct = constructAndStartCatalogTracker(connection);
      ServerName hsa = ct.getMetaLocation();
      Assert.assertNull(hsa);

      // Now test waiting on meta location getting set.
      Thread t = new WaitOnMetaThread(ct) {
        @Override
        void doWaiting() throws InterruptedException {
          this.ct.waitForMeta();
        }
      };
      startWaitAliveThenWaitItLives(t, 1000);

      // This should trigger wake up of meta wait (Its the removal of the meta
      // region unassigned node that triggers catalogtrackers that a meta has
      // been assigned).
      String node = ct.getMetaNodeTracker().getNode();
      ZKUtil.createAndFailSilent(this.watcher, node);
      MetaEditor.updateMetaLocation(ct, HRegionInfo.FIRST_META_REGIONINFO, SN, 0);
      ZKUtil.deleteNode(this.watcher, node);
      // Go get the new meta location. waitForMeta gets and verifies meta.
      Assert.assertTrue(ct.waitForMeta(10000).equals(SN));
      // Join the thread... should exit shortly.
      t.join();
      // Now meta is available.
      Assert.assertTrue(ct.waitForMeta(10000).equals(SN));
    } finally {
      HConnectionManager.deleteConnection(UTIL.getConfiguration());
    }
  }

  /**
   * @param admin An {@link AdminProtocol} instance; you'll likely
   * want to pass a mocked HRS; can be null.
   * @param client A mocked ClientProtocol instance, can be null
   * @return Mock up a connection that returns a {@link Configuration} when
   * {@link HConnection#getConfiguration()} is called, a 'location' when
   * {@link HConnection#getRegionLocation(byte[], byte[], boolean)} is called,
   * and that returns the passed {@link AdminProtocol} instance when
   * {@link HConnection#getAdmin(String, int)} is called, returns the passed
   * {@link ClientProtocol} instance when {@link HConnection#getClient(String, int)}
   * is called (Be sure call
   * {@link HConnectionManager#deleteConnection(org.apache.hadoop.conf.Configuration)}
   * when done with this mocked Connection.
   * @throws IOException
   */
  private HConnection mockConnection(final AdminProtocol admin,
      final ClientProtocol client) throws IOException {
    HConnection connection =
      HConnectionTestingUtility.getMockedConnection(UTIL.getConfiguration());
    Mockito.doNothing().when(connection).close();
    // Make it so we return any old location when asked.
    final HRegionLocation anyLocation =
      new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, SN.getHostname(),
        SN.getPort());
    Mockito.when(connection.getRegionLocation((byte[]) Mockito.any(),
        (byte[]) Mockito.any(), Mockito.anyBoolean())).
      thenReturn(anyLocation);
    Mockito.when(connection.locateRegion((byte[]) Mockito.any(),
        (byte[]) Mockito.any())).
      thenReturn(anyLocation);
    if (admin != null) {
      // If a call to getHRegionConnection, return this implementation.
      Mockito.when(connection.getAdmin(Mockito.anyString(), Mockito.anyInt())).
        thenReturn(admin);
    }
    if (client != null) {
      // If a call to getClient, return this implementation.
      Mockito.when(connection.getClient(Mockito.anyString(), Mockito.anyInt())).
        thenReturn(client);
    }
    return connection;
  }

  /**
   * @return A mocked up Result that fakes a Get on a row in the
   * <code>.META.</code> table.
   * @throws IOException
   */
  private Result getMetaTableRowResult() throws IOException {
    return MetaMockingUtil.getMetaTableRowResult(HRegionInfo.FIRST_META_REGIONINFO, SN);
  }

  private void startWaitAliveThenWaitItLives(final Thread t, final int ms) {
    t.start();
    while(!t.isAlive()) {
      // Wait
    }
    // Wait one second.
    Threads.sleep(ms);
    Assert.assertTrue("Assert " + t.getName() + " still waiting", t.isAlive());
  }

  class CountingProgressable implements Progressable {
    final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public void progress() {
      this.counter.incrementAndGet();
    }
  }

  /**
   * Wait on META.
   * Default is wait on -ROOT-.
   */
  class WaitOnMetaThread extends Thread {
    final CatalogTracker ct;

    WaitOnMetaThread(final CatalogTracker ct) {
      super("WaitOnMeta");
      this.ct = ct;
    }

    @Override
    public void run() {
      try {
        doWaiting();
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed wait", e);
      }
      LOG.info("Exiting " + getName());
    }

    void doWaiting() throws InterruptedException {
      try {
        while (this.ct.waitForRoot(100) == null);
      } catch (NotAllMetaRegionsOnlineException e) {
        // Ignore.
      }
    }
  }

}
