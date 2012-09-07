package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

public class PutSimple {

  private static final byte[] TABLE_NAME = Bytes.toBytes("tab1A");
  private static final byte[] FAM_NAM = Bytes.toBytes("f1");
  private static final byte[] ROW = Bytes.toBytes("aaa");
  public static long insertDuration;
  public static Log LOG = LogFactory.getLog(PutSimple.class);

  @Test
  public void testPuts() throws Exception {
    HTable table = getExternalClusterTable();
    table.setAutoFlush(false);
    final int size = 10000000;
    Date debut = new Date();
    Random r = new Random(0);

   final String x = "9900000000";
   final int xl = x.length();

      for (int i = 0; i < size; i++) {
        long in = (long) (r.nextDouble() * 10000000000L);
        assert in > 0L;
        assert in < 10000000000L;
        String inS = "00000000000000000000" + in;
        String inS2 = "'" + inS.substring((inS.length() - xl), inS.length()) + "',";
        Put put = new Put(Bytes.toBytes(inS2));
        put.add(FAM_NAM, ROW, ROW);
        table.put(put);
    }
    table.close();
    insertDuration = new Date().getTime() - debut.getTime();
  }

  public static HTable getExternalClusterTable() throws IOException {
    org.apache.hadoop.conf.Configuration c = HBaseConfiguration.create();

    c.set("hbase.zookeeper.quorum", "127.0.0.1");
    c.setInt("hbase.zookeeper.property.clientPort", 2181);

    return new HTable(c, TABLE_NAME);
  }

  public static void main(String[] args) throws Exception {
    new PutSimple().testPuts();
    System.out.println(" insertion duration " + (insertDuration / 1000) + " seconds");
  }
}
