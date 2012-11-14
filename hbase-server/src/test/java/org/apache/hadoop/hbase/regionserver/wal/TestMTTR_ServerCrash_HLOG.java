package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.HBaseRecoveryTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * JUnit test case for HLog
 */
@Category(LargeTests.class)
public class TestMTTR_ServerCrash_HLOG {
  private final static HBaseRecoveryTestingUtility rtu = new HBaseRecoveryTestingUtility();

  private static class WriterThread extends Thread {
    volatile boolean stop = false;
    volatile long time;

    public void run() {
      final long start = System.currentTimeMillis();
      try {
        HBaseRecoveryTestingUtility.TestPuts tp = rtu.new TestPuts(100000);
        tp.checkPuts();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final long end = System.currentTimeMillis();
      time = end - start;
      stop = true;
    }
  }

  @Test
  public void test1() throws Exception {
    rtu.startClusterSynchronous(1, 2);
    rtu.createTableWithRegionsOnRS(1, 0);

    HBaseRecoveryTestingUtility.TestPuts tp = rtu.new TestPuts(1);
    System.out.println("************* start first put");
    tp.checkPuts();

    System.out.println("************* start new DN");
    rtu.startNewDatanode();

    System.out.println("************* kill first DN");
    rtu.stopCleanDataNode(1);

    final long start = System.currentTimeMillis();
    System.out.println("************* new put again");
    tp = rtu.new TestPuts(1);
    tp.checkPuts();
    final long end = System.currentTimeMillis();
    long time = end - start;

    System.out.println("*****");
    System.out.println("***************************************** Time=" + time + " lines=" + rtu.getNbPuts());
    System.out.println("*****");
    System.out.flush();
    //rtu.killMyProcess();
    rtu.stopCleanCluster();
  }
}