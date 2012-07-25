/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.fs;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
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
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.ServerSocket;

@Category(LargeTests.class)
public class TestBlockReorder {
  private static final Log LOG = LogFactory.getLog(TestBlockReorder.class);

  static {
    //((Log4JLogger) DataNode.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger) LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) HFileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

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
    TEST_UTIL.getConfiguration().setInt("dfs.replication", 3);
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


  /**
   * Tests that we're can add a hook, and that this hook works when we try to read the file in HDFS.
   */
  //@Test
  public void testBlockLocationReorder() throws Exception {
    Path p = new Path("hello");

    Assert.assertTrue((short) cluster.getDataNodes().size() > 1);
    final int repCount = 2;

    // Let's write the file
    FSDataOutputStream fop = dfs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();

    // Let's check we can read it when everybody's there
    long start = System.currentTimeMillis();
    FSDataInputStream fin = dfs.open(p);
    Assert.assertTrue(toWrite == fin.readDouble());
    long end = System.currentTimeMillis();
    LOG.fatal("HFileSystem readtime= " + (end - start));
    fin.close();
    Assert.assertTrue((end - start) < 30 * 1000);

    // Let's kill the first location. But actually the fist location returned will change
    // The first thing to do it to get the location, then the port
    FileStatus f = dfs.getFileStatus(p);
    BlockLocation[] lbs;
    do {
      lbs = dfs.getFileBlockLocations(f, 0, 1);
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
    HFileSystem.addLocationOrderHack(conf,
        new HFileSystem.ReorderBlocks() {
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

    // Now it will fail with a timeout, unfortunately it does not always connect to the same box,
    // so we try 10 times;  with the reorder it will never last more than a few milli seconds
    boolean iAmBad = false;
    for (int i = 0; i < 10 && !iAmBad; i++) {
      start = System.currentTimeMillis();
      fin = dfs.open(p);
      Assert.assertTrue(toWrite == fin.readDouble());
      fin.close();
      end = System.currentTimeMillis();
      LOG.fatal("HFileSystem readtime= " + (end - start));
      if ((end - start) > 60000) iAmBad = true;
    }
    Assert.assertFalse("We took too much time to read", iAmBad);
    ss.close();
  }

  /**
   * Test that the hook works within HBase
   */
  @Test()
  public void testHBaseCluster() throws Exception {
    byte[] sb = "sb".getBytes();
    TEST_UTIL.startMiniZKCluster();
    MiniHBaseCluster hbm = TEST_UTIL.startMiniHBaseCluster(1, 2);
    HTable h = TEST_UTIL.createTable("table".getBytes(), sb);

    Put p = new Put(sb);
    p.add(sb, sb, sb);
    h.put(p);

    // Now we need to find the log file, its locations, and stop it
    String rootDir = conf.get(HConstants.HBASE_DIR) + "/" + HConstants.HREGION_LOGDIR_NAME;
    LOG.info("Reading " + rootDir);
    FileStatus[] fss = dfs.globStatus(new Path(rootDir));
    for (FileStatus f : fss) {
      LOG.info("File=" + f.getPath());
    }

    DirectoryListing dl = dfs.getClient().listPaths("/"+rootDir, HdfsFileStatus.EMPTY_NAME);
    Assert.assertNotNull(dl);
    HdfsFileStatus[] hfs = dl.getPartialListing() ;
    Assert.assertEquals(hfs.length, 1);
    Assert.assertNotNull(hfs[0]);


    /*
    org.apache.hadoop.fs.BlockLocation[] bls;
    long max = System.currentTimeMillis() + 10000;
    do {
      // Always returns 0, why?
      bls = dfs.getFileBlockLocations(fss[0], 0, 1);
      Assert.assertNotNull(bls);
      Assert.assertTrue("Expecting 3, got " + bls.length+" for file"+fss[0].getPath().getName(),System.currentTimeMillis() < max);
    } while (bls.length != 3);
    */


    // The interceptor is not set in this test, so we get the raw list
    LocatedBlocks l;
    final long max = System.currentTimeMillis() + 10000;
    do {
      // The "/" is mandatory, without it we've got a null pointer exception on the namenode
      l = dfs.getClient().namenode.getBlockLocations("/" + hfs[0].getLocalName(), 0, 1);
      Assert.assertNotNull("Trying " + "/" + hfs[0].getLocalName(), l);
      Assert.assertNotNull(l.getLocatedBlocks());
      Assert.assertEquals(l.getLocatedBlocks().size(), 1);
      Assert.assertTrue("Expecting " + 3 + " , got " + l.get(0).getLocations().length,
          System.currentTimeMillis() < max);
    } while (l.get(0).getLocations().length != 3);

  }

  /**
   * Test that the reorder algo works as we expect.
   */
  @Test
  public void testBlockLocation() throws Exception {
    final String fileName = "/helloWorld";
    Path p = new Path(fileName);

    final int repCount = 3;
    Assert.assertTrue((short) cluster.getDataNodes().size() >= repCount);

    // Let's write the file
    FSDataOutputStream fop = dfs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();


    // The interceptor is not set in this test, so we get the raw list
    LocatedBlocks l;
    final long max = System.currentTimeMillis() + 10000;
    do {
      l = dfs.getClient().namenode.getBlockLocations(fileName, 0, 1);
      Assert.assertNotNull(l.getLocatedBlocks());
      Assert.assertEquals(l.getLocatedBlocks().size(), 1);
      Assert.assertTrue("Expecting " + repCount + " , got " + l.get(0).getLocations().length,
          System.currentTimeMillis() < max);
    } while (l.get(0).getLocations().length != repCount);


    // Let's fix our own order
    setOurOrder(l);

    HFileSystem.LogReorderBlocks lrb = new HFileSystem.LogReorderBlocks();
    // Should be filtered, the name is different
    lrb.reorderBlocks(conf, l, fileName);
    checkOurOrder(l);

    // Should be reordered, as we pretend to be a file name with a compliant stuff
    Assert.assertNotNull(conf.get(HConstants.HBASE_DIR));
    Assert.assertFalse(conf.get(HConstants.HBASE_DIR).isEmpty());
    String pseudoLogFile = conf.get(HConstants.HBASE_DIR) + "/" +
        HConstants.HREGION_LOGDIR_NAME + "/" + host1 + ",6977,65766576";

    // Check that it will be possible to extract a ServerName from our construction
    Assert.assertNotNull(HLog.getServerNameFromHLogDirectoryName(conf, pseudoLogFile));

    // And check we're doing the right reorder.
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);

    // And change again and check again
    l.get(0).getLocations()[0].setHostName(host2);
    l.get(0).getLocations()[1].setHostName(host1);
    l.get(0).getLocations()[2].setHostName(host3);
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);

    // And change again and check again
    l.get(0).getLocations()[0].setHostName(host2);
    l.get(0).getLocations()[1].setHostName(host1);
    l.get(0).getLocations()[2].setHostName(host3);
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);

    // nothing to do here
    l.get(0).getLocations()[0].setHostName(host2);
    l.get(0).getLocations()[1].setHostName(host3);
    l.get(0).getLocations()[2].setHostName(host1);
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);

    // nothing to do here
    l.get(0).getLocations()[0].setHostName(host2);
    l.get(0).getLocations()[1].setHostName(host3);
    l.get(0).getLocations()[2].setHostName("nothing");
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    Assert.assertEquals(host2, l.get(0).getLocations()[0].getHostName());
    Assert.assertEquals(host3, l.get(0).getLocations()[1].getHostName());
    Assert.assertEquals("nothing", l.get(0).getLocations()[2].getHostName());
  }

  private void setOurOrder(LocatedBlocks l) {
    l.get(0).getLocations()[0].setHostName(host1);
    l.get(0).getLocations()[1].setHostName(host2);
    l.get(0).getLocations()[2].setHostName(host3);
  }

  private void checkOurOrder(LocatedBlocks l) {
    Assert.assertEquals(host1, l.get(0).getLocations()[0].getHostName());
    Assert.assertEquals(host2, l.get(0).getLocations()[1].getHostName());
    Assert.assertEquals(host3, l.get(0).getLocations()[2].getHostName());
  }

  private void checkOurFixedOrder(LocatedBlocks l) {
    Assert.assertEquals(host2, l.get(0).getLocations()[0].getHostName());
    Assert.assertEquals(host3, l.get(0).getLocations()[1].getHostName());
    Assert.assertEquals(host1, l.get(0).getLocations()[2].getHostName());
  }
}
