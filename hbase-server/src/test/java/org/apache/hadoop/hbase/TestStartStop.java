package org.apache.hadoop.hbase;

import com.google.protobuf.ServiceException;
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
  private HBaseTestingUtility htu = new HBaseTestingUtility();
  final byte[] TABLE_NAME1 = Bytes.toBytes("TestStartStop1");
  final byte[] FAM_NAME = Bytes.toBytes("fam");
  HTable table1;

  private void putData() throws IOException, InterruptedException, ServiceException {
    final byte[] QUAL_NAME = Bytes.toBytes("qual");
    final byte[] VALUE = Bytes.toBytes("value");

    table1 = htu.createTable(
        TABLE_NAME1, new byte[][]{FAM_NAME}, 3,
        Bytes.toBytes(0), Bytes.toBytes(Long.MAX_VALUE), 200);

    htu.waitTableEnabled(TABLE_NAME1, 30000);

    Random rd = new Random();
    for (int i = 0; i < 10000; ++i) {
      Put put = new Put(Bytes.toBytes(rd.nextLong()));
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
    MiniDFSCluster dfsCluster = htu.startMiniDFSCluster(8, null);
    hbaseCluster = htu.startMiniHBaseCluster(1, 8);
    hba = new HBaseAdmin(htu.getConfiguration());
    hba.setBalancerRunning(false, true);
    List<JVMClusterUtil.RegionServerThread> rs;
    do {
      Thread.sleep(200);
      rs = hbaseCluster.getLiveRegionServerThreads();
    } while (rs.size() != 8);

    putData();

    JVMClusterUtil.MasterThread master = hbaseCluster.getLiveMasterThreads().get(0);
    hbaseCluster.getMaster().shutdown();

    boolean stillAlive;
    do {
      Thread.sleep(200);
      stillAlive = master.isAlive();
      for (JVMClusterUtil.RegionServerThread rt : rs) {
        if (rt.isAlive()) {
          stillAlive = true;
        }
      }
    } while (stillAlive);

    dfsCluster.shutdown();
    zkCluster.shutdown();
  }
}

