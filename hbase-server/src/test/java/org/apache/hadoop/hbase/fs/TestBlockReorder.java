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
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import sun.rmi.runtime.NewThreadAction;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the hdfs fix from HBASE-6435.
 */
@Category(LargeTests.class)
public class TestBlockReorder {
  private static final Log LOG = LogFactory.getLog(TestBlockReorder.class);

  static {
    ((Log4JLogger) DFSClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger) HFileSystem.LOG).getLogger().setLevel(Level.ALL);
  }

  private Configuration conf;
  private MiniDFSCluster cluster;
  private HBaseTestingUtility htu;
  private DistributedFileSystem dfs;
  private final String host1 = "host1";
  private final String host2 = "host2";
  private final String host3 = "host3";

  @Before
  public void setUp() throws Exception {
    //host1 = InetAddress.getByName("127.0.0.1").getHostName();
    String h = guessHBaseHostname();
    LOG.info("My locahost name is "+h);
    // A trick to active block reorder on the unit tests. We want to have the same name for the
    //  hdfs node name and the hbase regionserver name.

    htu = new HBaseTestingUtility();
    htu.getConfiguration().setInt("dfs.block.size", 1024);// For the test with multiple blocks
    htu.getConfiguration().setBoolean("dfs.support.append", true);
    htu.getConfiguration().setInt("dfs.replication", 3);
    // We have a rack to get always the same location order but it does not work.
    htu.startMiniDFSCluster(3,
        new String[]{"/r1", "/r2", "/r3"}, new String[]{host1, host2, host3});

    conf = htu.getConfiguration();
    cluster = htu.getDFSCluster();
    dfs = (DistributedFileSystem) FileSystem.get(conf);
  }

  @After
  public void tearDownAfterClass() throws Exception {
    htu.shutdownMiniCluster();
  }


  /**
   * Test that we're can add a hook, and that this hook works when we try to read the file in HDFS.
   */
  @Test
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
    LOG.info("readtime= " + (end - start));
    fin.close();
    Assert.assertTrue((end - start) < 30 * 1000);

    // Let's kill the first location. But actually the fist location returned will change
    // The first thing to do it to get the location, then the port
    FileStatus f = dfs.getFileStatus(p);
    BlockLocation[] lbs;
    do {
      lbs = dfs.getFileBlockLocations(f, 0, 1);
    } while (lbs.length != 1 && lbs[0].getLength() != repCount);
    final String name = lbs[0].getNames()[0];
    Assert.assertTrue(name.indexOf(':') > 0);
    String portS = name.substring(name.indexOf(':') + 1);
    final int port = Integer.parseInt(portS);
    LOG.info("port= " + port);
    int ipcPort = -1;

    // Let's find the DN to kill. cluster.getDataNodes(int) is not on the same port, so wee need
    // to iterate ourselves.
    boolean ok = false;
    final String lookup = lbs[0].getHosts()[0];
    StringBuilder sb = new StringBuilder();
    for (DataNode dn : cluster.getDataNodes()) {
      final String dnName = getHostName(dn);
      sb.append(dnName).append(' ');
      if (lookup.equals(dnName)) {
        ok = true;
        LOG.info("killing datanode " + name + " / " + lookup);
        ipcPort = dn.ipcServer.getListenerAddress().getPort();
        dn.shutdown();
        LOG.info("killed datanode " + name + " / " + lookup);
        break;
      }
    }
    Assert.assertTrue("didn't find the server to kill, was looking for " + lookup + " found " + sb, ok);
    LOG.info("ipc port= " + ipcPort);

    // Add the hook, with an implementation checking that we don't use the port we've just killed.
    HFileSystem.addLocationOrderInterceptor(conf,
        new HFileSystem.ReorderBlocks() {
          @Override
          public void reorderBlocks(Configuration c, LocatedBlocks lbs, String src) {
            for (LocatedBlock lb : lbs.getLocatedBlocks()) {
              if (lb.getLocations().length > 1) {
                if (lb.getLocations()[0].getHostName().equals(lookup)) {
                  LOG.info("HFileSystem bad host, inverting");
                  DatanodeInfo tmp = lb.getLocations()[0];
                  lb.getLocations()[0] = lb.getLocations()[1];
                  lb.getLocations()[1] = tmp;
                }
              }
            }
          }
        });

    ServerSocket ss = new ServerSocket(port);// We're taking the port to have a timeout issue later.
    ServerSocket ssI = new ServerSocket(ipcPort);

    // Now it will fail with a timeout, unfortunately it does not always connect to the same box,
    // so we try 10 times;  with the reorder it will never last more than a few milli seconds
    for (int i = 0; i < 10; i++) {
      start = System.currentTimeMillis();

      fin = dfs.open(p);
      Assert.assertTrue(toWrite == fin.readDouble());
      fin.close();
      end = System.currentTimeMillis();
      LOG.info("HFileSystem readtime= " + (end - start));
      Assert.assertFalse("We took too much time to read", (end - start) > 60000);
    }
    ss.close();
    ssI.close();
  }

  /**
   * Allow to get the hostname, using getHostName (hadoop 1) or getDisplayName (hadoop 2)
   */
  private String getHostName(DataNode dn) throws InvocationTargetException, IllegalAccessException {
    Method m;
    try {
      m = DataNode.class.getMethod("getDisplayName");
    } catch (NoSuchMethodException e) {
      try {
        m = DataNode.class.getMethod("getHostName");
      } catch (NoSuchMethodException e1) {
        throw new RuntimeException(e1);
      }
    }

    String res = (String) m.invoke(dn);
    if (res.contains(":")) {
      return res.split(":")[0];
    } else {
      return res;
    }
  }
  static class SocketThread extends Thread{
    public volatile boolean done = false;
    private AtomicInteger port = new AtomicInteger(-1);
    public void run(){
      try {
        ServerSocket ss = new ServerSocket();
        port.set( ss.getLocalPort() );
        ss.accept();
        while(!done){
          Thread.sleep(1);
        }
        ss.close();
      } catch (Exception e) {
        LOG.error(e);
      }
    }
    public int getPort() throws InterruptedException {
      while (port.get() <0){
        Thread.sleep(1);
      }
      return port.get();
    }
  }

  public String guessHBaseHostname() throws Exception {
    SocketThread st = new SocketThread();
    st.start();
    int port = st.getPort();
    Socket s = new Socket("127.0.0.1", port);
    String res = s.getInetAddress().getHostName();
    s.close();
    st.done = true;
    return res;
  }

  /**
   * Test that the hook works within HBase, including when there are multiple blocks.
   */
  @Test()
  public void testHBaseCluster() throws Exception {
    byte[] sb = "sb".getBytes();
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 1);
    hbm.waitForActiveAndReadyMaster();
    hbm.getRegionServer(0).waitForServerOnline();

    // We want to have one with the same name as the rs to check the reorder
    final String rsHostname = hbm.getRegionServer(0).getServerName().getHostname();
    LOG.info("Starting a new datanode with name="+rsHostname);
    cluster.startDataNodes(
        conf, 1, true, null, new String[]{"/r4"}, new String[]{rsHostname}, null);
    cluster.waitClusterUp();
    // We want only 3 datanodes to be sure the new one will used
    cluster.stopDataNode(0);

    // We use the regionserver file system & conf as we expect it to have the hook.
    conf = hbm.getRegionServer(0).getConfiguration();
    HFileSystem rfs = (HFileSystem) hbm.getRegionServer(0).getFileSystem();
    HTable h = htu.createTable("table".getBytes(), sb);

    // Insert enough data to get multiple blocks
    for (int i = 0; i < 1024; i++) {
      Put p = new Put(sb);
      p.add(sb, sb, sb);
      h.put(p);
    }

    // Now we need to find the log file, its locations, and stop it
    String rootDir = FileSystem.get(conf).makeQualified(new Path(
        conf.get(HConstants.HBASE_DIR) + "/" + HConstants.HREGION_LOGDIR_NAME +
            "/" + hbm.getRegionServer(0).getServerName().toString())).toUri().getPath();

    DirectoryListing dl = dfs.getClient().listPaths(rootDir, HdfsFileStatus.EMPTY_NAME);
    // If we don't find anything it means that we're wrong on the path naming rules
    Assert.assertNotNull("Can't find: " + rootDir, dl);
    HdfsFileStatus[] hfs = dl.getPartialListing();

    // As we wrote a put, we should have at least one log file.
    Assert.assertTrue(hfs.length >= 1);
    for (HdfsFileStatus hf : hfs) {
      LOG.info("Log file found: " + hf.getLocalName() + " in " + rootDir);
    }

    // We will try only one file
    Assert.assertNotNull(hfs[hfs.length - 1]);
    String logFile = rootDir + "/" + hfs[0].getLocalName();
    LOG.info("Checking log file: " + logFile);

    // Checking the underlying file system. Multiple times as the order is random
    testFromDFS(dfs, logFile);

    // Now checking that the hook is up and running
    // We can't call directly getBlockLocations, it's not available in HFileSystem
    // We're trying ten times to be sure, as the order is random
    FileStatus fsLog = rfs.getFileStatus(new Path(logFile));
    for (int i = 0; i < 10; i++) {
      BlockLocation[] blocs;
      // The NN gets the block list asynchronously, so we may need multiple tries to get the list
      final long max = System.currentTimeMillis() + 10000;
      do {
        Assert.assertTrue("Can't get enouth replica.", System.currentTimeMillis() < max);
        blocs = rfs.getFileBlockLocations(fsLog, 0, 1);
        Assert.assertNotNull("Can't get block locations for " + logFile, blocs);
        Assert.assertTrue(blocs.length > 0);
      } while (blocs[0].getHosts().length != 3);

      Assert.assertEquals(host1, blocs[0].getHosts()[2]);
      Assert.assertNotSame(host1, blocs[0].getHosts()[1]);
      Assert.assertNotSame(host1, blocs[0].getHosts()[0]);
    }

    // now from the master
    DistributedFileSystem mdfs = (DistributedFileSystem)
        hbm.getMaster().getMasterFileSystem().getFileSystem();
    testFromDFS(mdfs, logFile);
  }

  private void testFromDFS(DistributedFileSystem dfs, String src) throws Exception {
    // Multiple times as the order is random
    for (int i = 0; i < 10; i++) {
      LocatedBlocks l;
      // The NN gets the block list asynchronously, so we may need multiple tries to get the list
      final long max = System.currentTimeMillis() + 10000;
      boolean done;
      do {
        Assert.assertTrue("Can't get enouth replica.", System.currentTimeMillis() < max);
        l = getNamenode(dfs.getClient()).getBlockLocations(src, 0, 1);
        Assert.assertNotNull("Can't get block locations for " + src, l);
        Assert.assertNotNull(l.getLocatedBlocks());
        Assert.assertTrue(l.getLocatedBlocks().size() > 0);

        done = true;
        for (int y = 0; y < l.getLocatedBlocks().size() && done; y++) {
          done = (l.get(y).getLocations().length == 3);
        }
      } while (!done);

      for (int y = 0; y < l.getLocatedBlocks().size() && done; y++) {
        Assert.assertEquals(host1, l.get(y).getLocations()[2].getHostName());
      }
    }
  }

  private static ClientProtocol getNamenode(DFSClient dfsc) throws Exception {
    Field nf = DFSClient.class.getDeclaredField("namenode");
    nf.setAccessible(true);
    return (ClientProtocol) nf.get(dfsc);
  }

  /**
   * Test that the reorder algo works as we expect.
   */
  @Test
  public void testBlockLocation() throws Exception {
    // We need to start HBase to get  HConstants.HBASE_DIR set in conf
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 1);
    conf = hbm.getConfiguration();


    // The "/" is mandatory, without it we've got a null pointer exception on the namenode
    final String fileName = "/helloWorld";
    Path p = new Path(fileName);

    final int repCount = 3;
    Assert.assertTrue((short) cluster.getDataNodes().size() >= repCount);

    // Let's write the file
    FSDataOutputStream fop = dfs.create(p, (short) repCount);
    final double toWrite = 875.5613;
    fop.writeDouble(toWrite);
    fop.close();


    // The interceptor is not set in this test, so we get the raw list at this point
    LocatedBlocks l;
    final long max = System.currentTimeMillis() + 10000;
    do {
      l = getNamenode(dfs.getClient()).getBlockLocations(fileName, 0, 1);
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
        HConstants.HREGION_LOGDIR_NAME + "/" + host1 + ",6977,6576" + "/mylogfile";

    // Check that it will be possible to extract a ServerName from our construction
    Assert.assertNotNull("log= " + pseudoLogFile,
        HLog.getServerNameFromHLogDirectoryName(dfs.getConf(), pseudoLogFile));

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

    // nothing to do here, but let's check
    l.get(0).getLocations()[0].setHostName(host2);
    l.get(0).getLocations()[1].setHostName(host3);
    l.get(0).getLocations()[2].setHostName(host1);
    lrb.reorderBlocks(conf, l, pseudoLogFile);
    checkOurFixedOrder(l);

    // nothing to do here, check again
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
