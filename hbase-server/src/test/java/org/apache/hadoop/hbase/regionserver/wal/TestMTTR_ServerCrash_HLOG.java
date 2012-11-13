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

    public void run() {
      while (!stop) {
        try {
          HBaseRecoveryTestingUtility.TestPuts tp = rtu.new TestPuts(100);
          tp.checkPuts();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @Test
  public void test1() throws Exception {
    rtu.startClusterSynchronous(3, 3);
    rtu.createTableWithRegionsOnRS(3, 3);

    HBaseRecoveryTestingUtility.TestPuts tp = rtu.new TestPuts(100);
    //tp.checkPuts();

    final long start = System.currentTimeMillis();

    WriterThread wt = new WriterThread();
    //wt.start();
   // Thread.sleep(10000);
    wt.stop = true;
    Thread.sleep(1000);

    //rtu.testAllPuts();

    final long end = System.currentTimeMillis();

    System.out.println("************* Time=" + (end - start) + " lines=" + rtu.getNbPuts());
    System.out.flush();
    //rtu.killMyProcess();
    rtu.stopCleanCluster();
  }
}