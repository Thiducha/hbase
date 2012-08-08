package org.apache.hadoop.hbase.ipc;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseRecoveryTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Category(LargeTests.class)
public class Test_HBASE_6364 {
  private static final Log LOG = LogFactory.getLog(Test_HBASE_6364.class);

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ALL);
    Logger.getLogger(DFSClient.class).setLevel(Level.DEBUG);
  }

  private HBaseRecoveryTestingUtility hrtu = new HBaseRecoveryTestingUtility();


  @Test
  public void test_6364() throws Exception {
    LOG.info("Start");

    hrtu.startClusterSynchronous(1, 1);
    hrtu.startNewRegionServer();
    hrtu.moveTableTo(".META.", 1); // We will have only meta on this server

    hrtu.createTable(10, 0);

    //hrtu.stopDirtyRegionServerTakePorts(1); // Can't be used here. We want a connect timeout, not a read timeout.
    HBaseClient.sleep =
        hrtu.getHBaseCluster().getRegionServer(1).getRpcServer().getListenerAddress().getPort();
    hrtu.stopDirtyRegionServer(1);

    final long start = System.currentTimeMillis();

    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicInteger errors = new AtomicInteger(0);

    final int nbTest = 20;
    for (int i=0; i<nbTest; i++){
      Thread t = new Thread(){
        public void run(){
          try {
            HTable h = new HTable(hrtu.getConfiguration(), hrtu.getTestTableName());
            h.get(new Get(HConstants.EMPTY_START_ROW)) ;
            h.close();
          } catch (IOException e) {
            errors.incrementAndGet();
          } finally {
            counter.incrementAndGet();
          }
        }
      } ;
      t.start();
    }

    while(counter.get() < nbTest){ Thread.sleep(1); }

    LOG.info("Time: " + (System.currentTimeMillis()-start) + " nb errors: "+errors.get());
    HBaseClient.sleep = 0;
    LOG.info("Done");
  }

  @Test
  public void test_6364_2() throws Exception {
    LOG.info("Start");

    hrtu.startClusterSynchronous(1, 1);
    hrtu.startNewRegionServer();
    hrtu.moveTableTo(".META.", 1); // We will have only meta on this server

    final int nbRS = 40; // Get 20 new servers an split a table on them
    for (int i=0; i<nbRS; i++) hrtu.startNewRegionServer();
    hrtu.createTable(nbRS, 2, 1+nbRS); // with the move, the cache gets partially broken

    HBaseClient.sleep =
        hrtu.getHBaseCluster().getRegionServer(1).getRpcServer().getListenerAddress().getPort();
    hrtu.stopDirtyRegionServer(1);

    final long start = System.currentTimeMillis();
    hrtu.new TestPuts(100).checkPuts();
    LOG.info("Time: "+(System.currentTimeMillis()-start));
    HBaseClient.sleep = 0;

    LOG.info("Done");
  }

}