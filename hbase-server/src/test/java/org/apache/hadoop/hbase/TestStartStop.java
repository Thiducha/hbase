package org.apache.hadoop.hbase;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;


public class TestStartStop {
  private final static Log LOG = LogFactory.getLog(TestStartStop.class);
  private HBaseTestingUtility htu = new HBaseTestingUtility();
  final byte[] TABLE_NAME1 = Bytes.toBytes("TestStartStop1");
  final byte[] TABLE_NAME2 = Bytes.toBytes("TestStartStop2");
  final byte[] FAM_NAME = Bytes.toBytes("fam");
  HTable table1;

  private void putData() throws IOException, InterruptedException, ServiceException {
    final byte[] QUAL_NAME = Bytes.toBytes("qual");
    final byte[] VALUE = Bytes.toBytes("value");

    table1 = htu.createTable(TABLE_NAME1, new byte[][]{FAM_NAME}, 3, "0".getBytes(), (Long.MAX_VALUE+"").getBytes(), 200);

    htu.waitTableEnabled(TABLE_NAME1, 30000);

    Random rd = new Random();
    for (int i = 0; i < 150000; ++i) {
      Put put = new Put(("" + rd.nextLong()).getBytes());
      put.add(FAM_NAME, QUAL_NAME, VALUE);
      table1.put(put);
    }
  }

  MiniHBaseCluster hbaseCluster;
  HBaseAdmin hba;

  @Test
  public void testSimpleStartStop() throws Exception {
    htu.getClusterTestDir();
    MiniZooKeeperCluster zkCluster = htu.startMiniZKCluster();
    MiniDFSCluster dfsCluster = htu.startMiniDFSCluster(3, null);
    hbaseCluster = htu.startMiniHBaseCluster(1, 3);
    hba = new HBaseAdmin(htu.getConfiguration());
    hba.setBalancerRunning(false, true);
    List<JVMClusterUtil.RegionServerThread> rs;
    do {
      Thread.sleep(200);
      rs = hbaseCluster.getLiveRegionServerThreads();
    } while (rs.size() != 3);

    putData();

    hbaseCluster.startRegionServer();
    hbaseCluster.startRegionServer();
    hbaseCluster.startRegionServer();
    hbaseCluster.startRegionServer();
    hbaseCluster.startRegionServer();
    do {
      Thread.sleep(200);
      rs = hbaseCluster.getLiveRegionServerThreads();
    } while (rs.size() != 8);



    htu.createTable(TABLE_NAME2, new byte[][]{FAM_NAME}, 3, "0".getBytes(), (Long.MAX_VALUE+"").getBytes(), 20);
    hba.split(TABLE_NAME1);
    int nb = rs.get(0).getRegionServer().getNumberOfOnlineRegions();
    hba.setBalancerRunning(true, true);
    hba.balancer();
    // 0.94.5 crashes if you do a split just after the balance (just invert the lines).
    do {
      Thread.sleep(1);
    } while (rs.get(0).getRegionServer().getNumberOfOnlineRegions() == nb);

    hbaseCluster.getMaster().shutdown();

    boolean ok;
    do {
      Thread.sleep(200);
      ok = true;
      for (JVMClusterUtil.RegionServerThread rt : rs) {
        if (rt.isAlive()) {
          ok = false;
        }
      }
    } while (!ok);

    dfsCluster.shutdown();
    zkCluster.shutdown();
  }
}

