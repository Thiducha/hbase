package org.apache.hadoop.hbase;

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


public class TestStartStop {
  private final static Log LOG = LogFactory.getLog(TestStartStop.class);
  private HBaseTestingUtility htu = new HBaseTestingUtility();

  private void putData() throws IOException, InterruptedException {
    final byte[] TABLE_NAME = Bytes.toBytes("test");
    final byte[] FAM_NAME = Bytes.toBytes("fam");
    final byte[] ROW = Bytes.toBytes("row");
    final byte[] QUAL_NAME = Bytes.toBytes("qual");
    final byte[] VALUE = Bytes.toBytes("value");

    HTable table1 = htu.createTable(TABLE_NAME, FAM_NAME);

    htu.waitTableEnabled(TABLE_NAME);

    Put put = new Put(ROW);
    put.add(FAM_NAME, QUAL_NAME, VALUE);
    table1.put(put);

    put = new Put("zfds".getBytes());
    put.add(FAM_NAME, QUAL_NAME, VALUE);
    table1.put(put);

    hba.split(TABLE_NAME);

    table1.close();
  }

  MiniHBaseCluster hbaseCluster;
  HBaseAdmin hba;

  @Test
  public void testSimpleStartStop() throws Exception {
    htu.getClusterTestDir();
    MiniZooKeeperCluster zkCluster = htu.startMiniZKCluster();
    MiniDFSCluster dfsCluster = htu.startMiniDFSCluster(3, null);
    hbaseCluster = htu.startMiniHBaseCluster(1, 8);
    hba = new HBaseAdmin(htu.getConfiguration());
    hba.setBalancerRunning(false, true);
    List<JVMClusterUtil.RegionServerThread> rs;
    do {
      Thread.sleep(200);
      rs = hbaseCluster.getLiveRegionServerThreads();
    } while (rs.size() != 8);
    putData();

    hbaseCluster.getMaster().shutdown();

    boolean ok;
    do {
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

