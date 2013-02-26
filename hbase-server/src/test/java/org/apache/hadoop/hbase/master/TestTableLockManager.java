/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.LockTimeoutException;
import org.apache.hadoop.hbase.exceptions.TableNotDisabledException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the default table lock manager
 */
@Category(MediumTests.class)
public class TestTableLockManager {

  private static final Log LOG =
    LogFactory.getLog(TestTableLockManager.class);

  private static final byte[] TABLE_NAME = Bytes.toBytes("TestTableLevelLocks");

  private static final byte[] FAMILY = Bytes.toBytes("f1");

  private static final byte[] NEW_FAMILY = Bytes.toBytes("f2");

  private final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  private static final CountDownLatch deleteColumn = new CountDownLatch(1);
  private static final CountDownLatch addColumn = new CountDownLatch(1);

  public void prepareMiniCluster() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
  }

  public void prepareMiniZkCluster() throws Exception {
    TEST_UTIL.startMiniZKCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 600000)
  public void testLockTimeoutException() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(TableLockManager.TABLE_WRITE_LOCK_TIMEOUT_MS, 3000);
    prepareMiniCluster();
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getCoprocessorHost().load(TestLockTimeoutExceptionMasterObserver.class,
        0, TEST_UTIL.getConfiguration());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Object> shouldFinish = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        admin.deleteColumn(TABLE_NAME, FAMILY);
        return null;
      }
    });

    deleteColumn.await();

    try {
      HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
      admin.addColumn(TABLE_NAME, new HColumnDescriptor(NEW_FAMILY));
      fail("Was expecting TableLockTimeoutException");
    } catch (LockTimeoutException ex) {
      //expected
    }
    shouldFinish.get();
  }

  public static class TestLockTimeoutExceptionMasterObserver extends BaseMasterObserver {
    @Override
    public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName, byte[] c) throws IOException {
      deleteColumn.countDown();
    }
    @Override
    public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName, byte[] c) throws IOException {
      Threads.sleep(10000);
    }

    @Override
    public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName, HColumnDescriptor column) throws IOException {
      fail("Add column should have timeouted out for acquiring the table lock");
    }
  }

  @Test(timeout = 600000)
  public void testAlterAndDisable() throws Exception {
    prepareMiniCluster();
    // Send a request to alter a table, then sleep during
    // the alteration phase. In the mean time, from another
    // thread, send a request to disable, and then delete a table.

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getCoprocessorHost().load(TestAlterAndDisableMasterObserver.class,
        0, TEST_UTIL.getConfiguration());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Object> alterTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        admin.addColumn(TABLE_NAME, new HColumnDescriptor(NEW_FAMILY));
        LOG.info("Added new column family");
        HTableDescriptor tableDesc = admin.getTableDescriptor(TABLE_NAME);
        assertTrue(tableDesc.getFamiliesKeys().contains(NEW_FAMILY));
        return null;
      }
    });
    Future<Object> disableTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
        admin.disableTable(TABLE_NAME);
        assertTrue(admin.isTableDisabled(TABLE_NAME));
        admin.deleteTable(TABLE_NAME);
        assertFalse(admin.tableExists(TABLE_NAME));
        return null;
      }
    });

    try {
      disableTableFuture.get();
      alterTableFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
      throw e;
    }
  }

  public static class TestAlterAndDisableMasterObserver extends BaseMasterObserver {
    @Override
    public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName, HColumnDescriptor column) throws IOException {
      LOG.debug("addColumn called");
      addColumn.countDown();
    }

    @Override
    public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName, HColumnDescriptor column) throws IOException {
      Threads.sleep(6000);
      try {
        ctx.getEnvironment().getMasterServices().checkTableModifiable(tableName);
      } catch(TableNotDisabledException expected) {
        //pass
        return;
      } catch(IOException ex) {
      }
      fail("was expecting the table to be enabled");
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName) throws IOException {
      try {
        LOG.debug("Waiting for addColumn to be processed first");
        //wait for addColumn to be processed first
        addColumn.await();
        LOG.debug("addColumn started, we can continue");
      } catch (InterruptedException ex) {
        LOG.warn("Sleep interrupted while waiting for addColumn countdown");
      }
    }

    @Override
    public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
        byte[] tableName) throws IOException {
      Threads.sleep(3000);
    }
  }

  @Test(timeout = 600000)
  public void testDelete() throws Exception {
    prepareMiniCluster();

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);

    //ensure that znode for the table node has been deleted
    ZooKeeperWatcher zkWatcher = TEST_UTIL.getZooKeeperWatcher();

    assertTrue(ZKUtil.checkExists(zkWatcher,
        ZKUtil.joinZNode(zkWatcher.tableLockZNode, Bytes.toString(TABLE_NAME))) < 0);

  }


  @Test(timeout = 600000)
  public void testReapAllTableLocks() throws Exception {
    prepareMiniZkCluster();
    ServerName serverName = new ServerName("localhost:10000", 0);
    final TableLockManager lockManager = TableLockManager.createTableLockManager(
        TEST_UTIL.getConfiguration(), TEST_UTIL.getZooKeeperWatcher(), serverName);

    String tables[] = {"table1", "table2", "table3", "table4"};
    ExecutorService executor = Executors.newFixedThreadPool(6);

    final CountDownLatch writeLocksObtained = new CountDownLatch(4);
    final CountDownLatch writeLocksAttempted = new CountDownLatch(10);
    //TODO: read lock tables

    //6 threads will be stuck waiting for the table lock
    for (int i = 0; i < tables.length; i++) {
      final String table = tables[i];
      for (int j = 0; j < i+1; j++) { //i+1 write locks attempted for table[i]
        executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            writeLocksAttempted.countDown();
            lockManager.writeLock(Bytes.toBytes(table), "testReapAllTableLocks").acquire();
            writeLocksObtained.countDown();
            return null;
          }
        });
      }
    }

    writeLocksObtained.await();
    writeLocksAttempted.await();

    //now reap all table locks
    lockManager.reapAllTableWriteLocks();

    TEST_UTIL.getConfiguration().setInt(TableLockManager.TABLE_WRITE_LOCK_TIMEOUT_MS, 0);
    TableLockManager zeroTimeoutLockManager = TableLockManager.createTableLockManager(
          TEST_UTIL.getConfiguration(), TEST_UTIL.getZooKeeperWatcher(), serverName);

    //should not throw table lock timeout exception
    zeroTimeoutLockManager.writeLock(Bytes.toBytes(tables[tables.length -1]), "zero timeout")
      .acquire();

    executor.shutdownNow();
  }

}
