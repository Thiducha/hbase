package org.apache.hadoop.hbase.fs;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.TestParallelPut;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.ServerSocket;

@Category(LargeTests.class)
public class TestBlockReorder {
  private static final Log LOG = LogFactory.getLog(TestBlockReorder.class);

  private Configuration conf;
  private MiniDFSCluster cluster;
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private DistributedFileSystem dfs;
  private final String host1 = "iambad";
  private final String host2 = "iamgood";
  private final String host3 = "iamverygood";

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // The reason to a rack it to try to get always the same order but it does not work.
    TEST_UTIL.startMiniDFSCluster(3, new String[]{"/r1", "/r2", "/r3"}, new String[]{host1, host2, host3});

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();
    dfs = (DistributedFileSystem) FileSystem.get(conf);
  }

  @After
  public void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }


  @Test
  public void testBlockLocationReorder() throws IOException, InterruptedException, NoSuchFieldException, IllegalAccessException {
    Path p = new Path("hello");

    Assert.assertTrue((short) cluster.getDataNodes().size() > 1);
    final int repCount = 2;

    // Let's write the file
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream fop = fs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();

    // Let's check we can read it when everybody's there
    long start = System.currentTimeMillis();
    FSDataInputStream fin = fs.open(p);
    Assert.assertTrue(toWrite == fin.readDouble());
    long end = System.currentTimeMillis();
    LOG.fatal("HFileSystem readtime= " + (end - start));
    fin.close();
    Assert.assertTrue((end - start) < 30 * 1000);

    // Let's kill the first location. But actually the fist location returned will change
    // The first thing to do it to get the location, then the port
    FileStatus f = fs.getFileStatus(p);
    BlockLocation[] lbs;
    do {
      lbs = fs.getFileBlockLocations(f, 0, 1);
    } while (lbs.length != 1 && lbs[0].getLength() != repCount);
    String name = lbs[0].getNames()[0];
    Assert.assertTrue(name.indexOf(':') > 0);
    String portS = name.substring(name.indexOf(':') + 1);
    final int port = Integer.parseInt(portS);
    LOG.fatal("HFileSystem port= " + port);

    // Let's find the DN to kill. cluster.getDataNodes(int) is not on the same port, so wee need
    // to iterate ourselves.
    boolean ok = false;
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.dnRegistration.getName().equals(name)) {
        ok = true;
        LOG.info("HFileSystem killing " + name);
        dn.shutdown();
        LOG.fatal("HFileSystem killed  " + name);
      }
    }
    Assert.assertTrue("didn't find the server to kill", ok);


    // Add the hook, with an implementation checking that we don't use the port we've just killed.
    HFileSystem.addLocationOrderHack(conf, new HFileSystem.ReorderBlocks() {
      @Override
      public void reorderBlocks(Configuration c, LocatedBlocks lbs, String src) {
        for (LocatedBlock lb : lbs.getLocatedBlocks()) {
          if (lb.getLocations().length > 1) {
            if (lb.getLocations()[0].getPort() == port) {
              LOG.fatal("HFileSystem bad port, inverting");
              DatanodeInfo tmp = lb.getLocations()[0];
              lb.getLocations()[0] = lb.getLocations()[1];
              lb.getLocations()[1] = tmp;
            }
          }
        }
      }
    });

    ServerSocket ss = new ServerSocket(port);   // We're taking the port to have a timeout issue.

    // Now it will fail with a timeout, unfortunately it does not always connect to the same box
    // With the reorder it will never fail
    boolean iAmBad = false;
    for (int i = 0; i < 10 && !iAmBad; i++) {
      start = System.currentTimeMillis();
      fin = fs.open(p);
      Assert.assertTrue(toWrite == fin.readDouble());
      fin.close();
      end = System.currentTimeMillis();
      LOG.fatal("HFileSystem readtime= " + (end - start));
      if ((end - start) > 60000) iAmBad = true;
    }
    Assert.assertFalse("We took too much time to read", iAmBad);
    ss.close();
  }

  @Test
  public void testHBaseCluster() throws Exception {
    byte[] sb = "sb".getBytes();
    MiniHBaseCluster hbm = TEST_UTIL.startMiniHBaseCluster(1, 2);
    HTable h = TEST_UTIL.createTable("table".getBytes(), sb);

    Put p = new Put(sb);
    p.add(sb, sb, sb);
    h.put(p);

    // Now we need to find the log file, its locations, and stop it
    FileSystem fs = FileSystem.get(conf);

    String rootDir = conf.get(HConstants.HBASE_DIR) + "/" + HConstants.HREGION_LOGDIR_NAME;
    FileStatus[] fss = fs.globStatus(new Path(rootDir));
    for (FileStatus f : fss) {
      LOG.info("File=" + f.getPath());
    }

    Assert.assertTrue(fss.length == 1);

    org.apache.hadoop.fs.BlockLocation[] bls = fs.getFileBlockLocations(fss[0], 0, 1);
    Assert.assertTrue(bls.length == 2);
  }

  @Test
  public void testBlockLocation() throws IOException {
    final String fileName = "helloWorld";
    Path p = new Path(fileName);

    Assert.assertTrue((short) cluster.getDataNodes().size() > 1);
    final int repCount = 2;

    // Let's write the file
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream fop = fs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();

    HFileSystem.LogReorderBlocks lrb = new HFileSystem.LogReorderBlocks();
    FileStatus f = fs.getFileStatus(p);

    // The interceptor is not set in this test, so we get the raw list
    LocatedBlocks l = dfs.getClient().namenode.getBlockLocations(fileName, 0, 1);
    Assert.assertTrue(l.getLocatedBlocks().size() == 1);
    Assert.assertTrue(l.get(0).getLocations().length == 3);

    // Let's fix our own order
    setOurOrder(l);

    // Should be filtered, the name is different
    lrb.reorderBlocks(conf, l, fileName);
    checkOurOrder(l);

    // Should be reordered
    String pseudoLogFile = conf.get(HConstants.HBASE_DIR) + "/" +
        HConstants.HREGION_LOGDIR_NAME+"/"+host1;
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);
  }

  private void setOurOrder(LocatedBlocks l) {
    l.get(0).getLocations()[0].setHostName(host1);
    l.get(0).getLocations()[1].setHostName(host2);
    l.get(0).getLocations()[2].setHostName(host3);
  }

  private void checkOurOrder(LocatedBlocks l) {
    Assert.assertTrue(host1.equals(l.get(0).getLocations()[0].getHostName()));
    Assert.assertTrue(host2.equals(l.get(0).getLocations()[1].getHostName()));
    Assert.assertTrue(host3.equals(l.get(0).getLocations()[2].getHostName()));
  }

  private void checkOurFixedOrder(LocatedBlocks l) {
    Assert.assertTrue(host2.equals(l.get(0).getLocations()[0].getHostName()));
    Assert.assertTrue(host3.equals(l.get(0).getLocations()[1].getHostName()));
    Assert.assertTrue(host1.equals(l.get(0).getLocations()[2].getHostName()));
  }
}
