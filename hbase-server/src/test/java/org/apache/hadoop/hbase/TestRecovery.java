package org.apache.hadoop.hbase;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

@Category(LargeTests.class)
public class TestRecovery {

  HBaseRecoveryTestingUtility TEST_UTIL = new HBaseRecoveryTestingUtility();

  private static final Log LOG = LogFactory.getLog(TestRecovery.class);
  private final int nbTests = 5;
  private final int nbMoves = 5;
  private final int nbVal = 50;

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
    Logger.getLogger(DFSClient.class).setLevel(Level.DEBUG);
    Logger.getLogger("org.apache.hadoop.hdfs.server.datanode").setLevel(Level.DEBUG);
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.stopCleanCluster();
  }


  // OK 6 Tests 2min53s
  @Test
  public void testKillRS() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      TEST_UTIL.startNewRegionServer();
      TEST_UTIL.stopDirtyRegionServer(0);
      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }

  // Ok 6 Tests 2min56s
  @Test
  public void testAbortRS() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      TEST_UTIL.startNewRegionServer();
      TEST_UTIL.abortRegionServer(0);
      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }

  // OK 6 Tests 7min55s
  @Test
  public void testKillDN() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      TEST_UTIL.stopDirtyDataNode(0);
      TEST_UTIL.stopDirtyDataNode(1);

      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }


  /**
   * Similulate a test that can be done on a real 3 nodes cluster as well.
   */
  @Test
  public void testKillOneDNandOneRS() throws Exception {
    // dfs.replication will be equals to 2
    TEST_UTIL.startClusterSynchronous(2, 1);

    // Put a 100 regions table on the second node
    TEST_UTIL.startNewRegionServer();
    TEST_UTIL.createTableWithRegionsOnRS(100, 1);

    // Insert puts, there will be on the memstore
    HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(100000);
    puts.checkPuts();

    // start new & kill on DN and the RS with the table on.
    TEST_UTIL.startNewDatanode();
    TEST_UTIL.startNewRegionServer();
    TEST_UTIL.stopDirtyDataNode(1);
    TEST_UTIL.stopDirtyRegionServer(1);

    final long start = System.currentTimeMillis();
    int nbLiveRegion = 0;
    do {
      Thread.sleep(1);
      nbLiveRegion = TEST_UTIL.getHBaseCluster().getRegionServer(0).getNumberOfOnlineRegions();
      nbLiveRegion += TEST_UTIL.getHBaseCluster().getRegionServer(2).getNumberOfOnlineRegions();
    } while (nbLiveRegion != 103);
    final long time = (System.currentTimeMillis() - start);

    System.out.println("time = " + time);
    puts.checkPuts();

    TEST_UTIL.stopCleanCluster();
  }


  // OK 6 Tests 10min32s
  @Test
  public void testStopDN() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      TEST_UTIL.stopCleanDataNode(0);
      TEST_UTIL.stopCleanDataNode(1);

      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }

  // Ok 6 Tests 3min32s
  @Test
  public void testFlushWithDeadDN() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      TEST_UTIL.startNewDatanode();
      TEST_UTIL.startNewDatanode();

      TEST_UTIL.stopCleanDataNode(3);
      TEST_UTIL.stopCleanDataNode(4);

      TEST_UTIL.flushSynchronous(0);

      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }

  // Fail
  // First iteration ok but not the second
  // checkPuts fail

/*
  Tests run: 1, Failures: 0, Errors: 1, Skipped: 0, Time elapsed: 548.272 sec <<< FAILURE!
testStopDNandRS(org.apache.hadoop.hbase.TestRecovery)  Time elapsed: 548.017 sec  <<< ERROR!
org.apache.hadoop.hbase.client.RetriesExhaustedException: Failed after attempts=20, exceptions:
Mon Sep 03 19:55:51 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
tion refused
Mon Sep 03 19:55:52 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
tion refused
Mon Sep 03 19:55:53 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, org.apache.hadoop.hbase.ipc.HBase
Client$FailedServerException: This server is in the failed servers list: localhost/127.0.0.1:32934
Mon Sep 03 19:55:54 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
tion refused
Mon Sep 03 19:55:56 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
tion refused
Mon Sep 03 19:55:58 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
tion refused
Mon Sep 03 19:56:02 CEST 2012, org.apache.hadoop.hbase.client.HTable$3@a6a2534, java.net.ConnectException: Connec
   */

  @Test
  public void testStopDNandRS() throws Exception {
    for (int i = 0; i < nbTests; i++) {
      LOG.info("Test " + i);
      TEST_UTIL.startClusterSynchronous(3, 3);
      TEST_UTIL.createTableWithRegionsOnRS(100, 0);

      // Search Regions Servers without Root and Meta
      int[] RS = TEST_UTIL.getRSNoRootAndNoMeta();

      // Move table to a Region Server without Root and Meta
      TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), RS[0]);

      // Insert
      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1000);
      puts.checkPuts();

      // Start & Kill 2 DNs out of the 3. There will be a replica available.
      TEST_UTIL.startNewDatanode();
      TEST_UTIL.startNewDatanode();
      TEST_UTIL.stopCleanDataNode(0);
      TEST_UTIL.stopCleanDataNode(1);

      // Kill all RS except the one with root or meta
      LOG.info("Killing servers # " + Arrays.toString(RS));
      for (int j = 0; j < RS.length; j++) {
        TEST_UTIL.stopDirtyRegionServer(RS[j]);
      }

      // We should find out puts!
      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + i);
    }
  }

  // Fail
  // Flush fail
  // testPutAndFlush(org.apache.hadoop.hbase.master.TestRecovery): org.apache.hadoop.hbase.DroppedSnapshotException:
  // region: rb_-1360161948,4\xB4\xB4\xB4\xB4\xB4\xB4\xB4\xB4\xB4,1344595556533.75387eaa1105faac71e4e0e83f8608a4.
  //
  // Could be that we created too much files? We're just flushing empty files here.
  /*
  java.io.IOException: Couldn't instantiate org.apache.zookeeper.ClientCnxnSocketNIO
        at org.apache.zookeeper.ZooKeeper.getClientCnxnSocket(ZooKeeper.java:1758)
        at org.apache.zookeeper.ZooKeeper.<init>(ZooKeeper.java:442)
  Caused by: java.io.IOException: Too many open files
        at sun.nio.ch.IOUtil.initPipe(Native Method)
   */
  @Test
  public void testPutAndFlush() throws Exception {
    LOG.info("testPutAndFlush");
    // start cluster with 6DN and 1RS
    TEST_UTIL.startClusterSynchronous(6, 1);

    TEST_UTIL.stopCleanDataNode(0);
    TEST_UTIL.stopCleanDataNode(1);

    TEST_UTIL.createTableWithRegionsOnRS(6, 0);

    TEST_UTIL.stopCleanDataNode(2);

    for (int i = 0; i < 100000; i++) {
      LOG.info("Start Put " + i);
      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(1);
      puts.checkPuts();
      LOG.info("Put " + i + " Finished");

      LOG.info("Start Flush " + i);
      TEST_UTIL.getHBaseAdmin().flush(TEST_UTIL.getTestTableName());
      TEST_UTIL.flushSynchronous(0);
      LOG.info("Flush " + i + " finished");
    }

    TEST_UTIL.stopCleanCluster();
    LOG.info("End testPutAndFlush");
  }

  // Ok 2min47
  @Test
  public void testMoveAndKillRS() throws Exception {
    for (int j = 0; j < nbTests; j++) {
      LOG.info("Test " + j);
      TEST_UTIL.startClusterSynchronous(3, 4);
      TEST_UTIL.createTableWithRegionsOnRS(10, 0);

      int[] numDest;

      // Search Regions Servers without root and meta
      numDest = TEST_UTIL.getRSNoRootAndNoMeta();

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(100);
      puts.checkPuts();

      for (int i = 0; i < nbMoves; i++) {
        TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), numDest[0]);

        puts.checkPuts();

        TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), numDest[1]);

        // Kill server with table
        TEST_UTIL.stopDirtyRegionServer(numDest[0]);

        puts.checkPuts();

        // Prepare next iteration
        TEST_UTIL.startNewRegionServer();

        numDest[0] = numDest[1];
        numDest[1] = i + 4;
      }

      TEST_UTIL.stopCleanCluster();
      LOG.info("End test " + j);
    }
  }

  // Ok 34min36s
  @Test
  public void testTimeToMove() throws Exception {
    for (int i = 0; i < nbVal; i++) {
      TEST_UTIL.startClusterSynchronous(3, 1);
      TEST_UTIL.createTableWithRegionsOnRS(4, 0);

      HBaseRecoveryTestingUtility.TestPuts puts = TEST_UTIL.new TestPuts(100000);
      puts.checkPuts();

      TEST_UTIL.startNewRegionServer();

      long start = System.currentTimeMillis();
      TEST_UTIL.moveTableTo(TEST_UTIL.getTestTableNameToString(), 1);
      long stop = System.currentTimeMillis();
      long res = stop - start;
      LOG.info("Time to move region attempt " + i + " : " + res + " ms");

      puts.checkPuts();

      TEST_UTIL.stopCleanCluster();
    }
  }

  // Ok
  @Test
  public void testTimeHLogWrite() throws Exception {
    for (int i = 0; i < nbVal; i++) {
      TEST_UTIL.startClusterSynchronous(3, 3);
      TEST_UTIL.createTableWithRegionsOnRS(4, 0);

      // Create Region
      HRegionInfo hri = TEST_UTIL.createRegion();

      Path filename = TEST_UTIL.createHLog(hri, 0);

      TEST_UTIL.checkHLog(1, filename, hri);

      TEST_UTIL.stopCleanCluster();
    }
  }

  // Ok
  @Test
  public void testTimeHLogWriteWithKillDN() throws Exception {
    for (int i = 0; i < nbVal; i++) {
      TEST_UTIL.startClusterSynchronous(3, 3);
      TEST_UTIL.createTableWithRegionsOnRS(4, 0);

      // Create Region
      HRegionInfo hri = TEST_UTIL.createRegion();

      Path filename = TEST_UTIL.createHLog(hri, 1);

      TEST_UTIL.checkHLog(1, filename, hri);

      TEST_UTIL.stopCleanCluster();
    }
  }
}