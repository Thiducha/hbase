package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


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
    hbaseCluster = htu.startMiniHBaseCluster(1, 3);
    hba = new HBaseAdmin(htu.getConfiguration());
    hba.setBalancerRunning(false, true);
    putData();

    hbaseCluster.getMaster().shutdown();
    while (!hbaseCluster.getLiveRegionServerThreads().isEmpty()) {
      Thread.sleep(200);
    }

    dfsCluster.shutdown();
    zkCluster.shutdown();
  }
}

