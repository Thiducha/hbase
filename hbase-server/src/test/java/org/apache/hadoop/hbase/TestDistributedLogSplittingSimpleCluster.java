package org.apache.hadoop.hbase.master;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;

@Category(LargeTests.class)
public class TestDistributedLogSplittingSimpleCluster {
  private static final Log LOG = LogFactory.getLog(TestDistributedLogSplittingSimpleCluster.class);

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private HBaseAdmin admin = null;
  private MiniHBaseCluster cluster = null;

  private HBaseTestingUtility TESTING_UTIL;
  private static byte[] cf = "cf".getBytes();

  private HTable ht;

  private String rowPrefix = "row_";

  private void doPuts() throws IOException {
    byte[] row = rowPrefix.getBytes();
    Put put = new Put(row);
    put.add(cf, cf, cf);
    ht.put(put);
    ht.put(put);
    ht.put(put);

    for (int i = 0; i < 10; i++) {
      put = new Put((rowPrefix + i).getBytes());
      put.add(cf, cf, cf);
      ht.put(put);
    }
  }

  public void checkPut() throws IOException {
    Assert.assertTrue(Arrays.equals(ht.get(new Get(rowPrefix.getBytes())).getRow(), rowPrefix.getBytes()));
    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(Arrays.equals(ht.get(new Get((rowPrefix + i).getBytes())).getRow(), (rowPrefix + i).getBytes()));
    }
  }

  private void startCluster(int NB_SERVERS) throws Exception {
    startCluster(NB_SERVERS, NB_SERVERS);
  }

  private void startCluster(int nbDN, int nbRS) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    conf.setInt("dfs.replication", nbDN >= 3 ? 3 : 1);
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt("hbase.client.retries.number", 20);
    conf.setInt("hbase.regionserver.optionallogflushinterval", 0);
    conf.setInt("hbase.hstore.compactionThreshold", 100000);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);

    TESTING_UTIL = new HBaseTestingUtility(conf);

    TESTING_UTIL.startMiniDFSCluster(nbDN);
    TESTING_UTIL.getDFSCluster().waitClusterUp();
    TESTING_UTIL.startMiniZKCluster();
    TESTING_UTIL.startMiniHBaseCluster(1, nbRS);

    cluster = TESTING_UTIL.getHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    while (cluster.getLiveRegionServerThreads().size() < nbRS) {
      Threads.sleep(1);
    }

    ht = TESTING_UTIL.createTable("table".getBytes(), cf);
  }

  @After
  public void after() throws Exception {
    ht.close();
    TESTING_UTIL.shutdownMiniCluster();
    TESTING_UTIL.shutdownMiniDFSCluster();
    cluster = null;
  }


  @Test
  public void testKillDNandRSNoKillMS() throws Exception {
    // start 5 dn and 3 rs
    startCluster(5, 3);

    // stop 2 dn = 3 dn remaining
    MiniDFSCluster.DataNodeProperties dnprop0 = TESTING_UTIL.getDFSCluster().stopDataNode(0);
    MiniDFSCluster.DataNodeProperties dnprop1 = TESTING_UTIL.getDFSCluster().stopDataNode(1);

    byte[] servername;
    int metaserv = TESTING_UTIL.getHBaseCluster().getServerWithMeta();

    if (metaserv == 0) {
      servername = cluster.getRegionServer(1).getServerName().getVersionedBytes();
    } else {
      servername = cluster.getRegionServer(0).getServerName().getVersionedBytes();
    }

    // Make sure that the region is on a different server
    byte[] row = "row1".getBytes();
    HRegionLocation region = ht.getRegionLocation(row, true);

    Put put = new Put(row);
    put.add(cf, cf, cf);
    ht.put(put);
    Assert.assertTrue(Arrays.equals(ht.get(new Get(row)).getRow(), row));

    TESTING_UTIL.getHBaseAdmin().move(region.getRegionInfo().getEncodedNameAsBytes(), servername);

    // start 2 dn = 5 dn remaining
    TESTING_UTIL.getDFSCluster().restartDataNode(dnprop0, false);
    TESTING_UTIL.getDFSCluster().restartDataNode(dnprop1, false);
    TESTING_UTIL.getDFSCluster().waitClusterUp();
    // stop 2 dn = 3 dn remaining
    TESTING_UTIL.getDFSCluster().stopDataNode(2);
    TESTING_UTIL.getDFSCluster().stopDataNode(3);
    // stop 2 rs = 1 rs remaining
    // Don't kill server with meta !

    if (metaserv == 0) {
      cluster.getRegionServer(1).kill();
      cluster.getRegionServer(2).kill();
    } else if (metaserv == 1) {
      cluster.getRegionServer(0).kill();
      cluster.getRegionServer(2).kill();
    } else {
      cluster.getRegionServer(0).kill();
      cluster.getRegionServer(1).kill();
    }

    Assert.assertTrue(Arrays.equals(ht.get(new Get(row)).getRow(), row));
  }
}

