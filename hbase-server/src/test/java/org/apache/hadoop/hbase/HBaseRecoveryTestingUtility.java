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

package org.apache.hadoop.hbase;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Random;


@SuppressWarnings("UnusedDeclaration")
public class HBaseRecoveryTestingUtility extends HBaseTestingUtility {
  private static final Log LOG = LogFactory.getLog(HBaseRecoveryTestingUtility.class);

  private byte[] rb = ("rb_" + new Random(System.currentTimeMillis()).nextInt()).getBytes();
  private HTable testTable;

  static {
    Logger.getLogger(DFSClient.class).setLevel(Level.DEBUG);
    // Beware: for HBaseClient the log path does not match the class path. The class is in
    // 'org.apache.hadoop.hbase.ipc'  but logs in 'org.apache.hadoop.ipc.HBaseClient'
    Logger.getLogger("org.apache.hadoop.ipc.HBaseClient").setLevel(Level.DEBUG);
  }

  public int[] getRSNoRootAndNoMeta() {
    final int nbRS = getHBaseCluster().getRegionServerThreads().size();
    final byte[] nameRootRegion = HRegionInfo.ROOT_REGIONINFO.getRegionName();
    final int numRootServ = getHBaseCluster().getServerWith(nameRootRegion);
    final int numMetaServ = getHBaseCluster().getServerWithMeta();


    int[] RS;
    if (numRootServ == numMetaServ) {
      RS = new int[nbRS - 1];
    } else {
      RS = new int[nbRS - 2];
    }

    int j = 0;
    for (int i = 0; i < nbRS && j < 2; i++) {
      if (i != numRootServ && i != numMetaServ) {
        RS[j] = i;
        j++;
      }
    }

    return RS;
  }


  public void startNewRegionServer() throws IOException {
    LOG.info("START startNewRegionServer ");
    HRegionServer newRS;
    newRS = getHBaseCluster().startRegionServer().getRegionServer();
    newRS.waitForServerOnline();
    LOG.info("DONE startNewRegionServer ");
  }

  public void stopCleanRegionServer(int rs) {
    LOG.info("START stopCleanRegionServer " + rs + " " + getHBaseCluster().getRegionServer(rs).getServerName());
    getHBaseCluster().stopRegionServer(rs);
    LOG.info("DONE stopCleanRegionServer " + rs + " stopped");
  }

  public void stopDirtyRegionServer(int rsPos) throws Exception {
    HRegionServer rs = getHBaseCluster().getRegionServer(rsPos);
    LOG.info("START stopDirtyRegionServer Region Server " + rsPos + " " + rs.getServerName());
    JVMClusterUtil.RegionServerThread rst =
        getHBaseCluster().hbaseCluster.getRegionServers().get(rsPos);
    LOG.info("Number of live regions " + rs.getCopyOfOnlineRegionsSortedBySize().size());
    for (HRegion r : rs.getCopyOfOnlineRegionsSortedBySize().values()) {
      LOG.info("Closing server with region " + r.getRegionNameAsString());
    }

    Method kill = HRegionServer.class.getDeclaredMethod("kill");
    kill.setAccessible(true);
    kill.invoke(rs);

    while (!rs.isStopped()) {
      Thread.sleep(1);
    }
    LOG.info("Server is stopped, waiting for the region server thread to finish");
    if (rst.isAlive()) {
      rst.join();
    }

    LOG.info("DONE stopDirtyRegionServer " + rsPos + " stopped");
  }

  private ArrayList<ServerSocket> portsTaken = new ArrayList<ServerSocket>();

  public void freePorts() throws IOException {
    LOG.info("START freePorts");
    for (ServerSocket sock : portsTaken) {
      int port = sock.getLocalPort();
      LOG.info("Close port " + port);
      sock.close();
      LOG.info("Port " + port + "closed");
    }
    LOG.info("DONE freePorts");
  }

  private void takePort(int port) {
    if (port <= 0) return;
    LOG.info("START takePort " + port);

    ServerSocket ss = null;
    do {
      try {
        ss = new ServerSocket(port);
      } catch (IOException ignored) {
      }
    } while (ss == null);
    portsTaken.add(ss);

    LOG.info("DONE takePort " + port);
  }

  private void takePortOnce(final int port) {
    if (port <= 0) return;
    LOG.info("START takePortOnce " + port);

    Thread t = new Thread() {
      public void run() {
        ServerSocket ss = null;
        do {
          try {
            ss = new ServerSocket(port);
          } catch (IOException ignored) {
          }
        } while (ss == null);
        portsTaken.add(ss);
      }
    };

    t.start();


    LOG.info("DONE takePortOnce " + port);
  }

  public void stopDirtyRegionServerTakePorts(int rs) throws Exception {
    LOG.info("START stopDirtyRegionServerTakePorts " + rs + " " +
        getHBaseCluster().getRegionServer(rs).getServerName());

    int rpcPort =
        getHBaseCluster().getRegionServer(rs).getRpcServer().getListenerAddress().getPort();
    int infoPort = -1;
    if (getHBaseCluster().getRegionServer(rs).getInfoServer() != null) {
      infoPort = getHBaseCluster().getRegionServer(rs).getInfoServer().getPort();
    }

    stopDirtyRegionServer(rs);

    takePort(rpcPort);
    takePort(infoPort);

    LOG.info("DONE stopDirtyRegionServerTakePorts " + rs);
  }

  public void stopDirtyDataNodeTakePorts(int dn) throws Exception {
    int rpcPort = getDFSCluster().getDataNodes().get(dn).ipcServer.getListenerAddress().getPort();
    int infoPort = getInfoPort(getDFSCluster().getDataNodes().get(dn));

    LOG.info("START stopDirtyDataNodeTakePorts " + dn + " " +
        getHostName( getDFSCluster().getDataNodes().get(dn)) + ":" +
        getDFSCluster().getDataNodes().get(dn).getRpcPort());

    stopDirtyDataNode(dn);

    takePort(rpcPort);
    takePort(infoPort);

    LOG.info("DONE stopDirtyDataNodeTakePorts " + dn);
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

  private int getInfoPort(DataNode dn) throws InvocationTargetException, IllegalAccessException {
    try {
      Method m = DataNode.class.getMethod("getXferPort");       // hdfs 2
      return (Integer)(m.invoke(dn));
    } catch (NoSuchMethodException e) {
      try {
        Method m = DataNode.class.getMethod("getSelfAddr");     // hdfs 1
        InetSocketAddress sa = (InetSocketAddress)m.invoke(dn);
        if (sa == null){
          return -1;
        }
        return sa.getPort();
      } catch (NoSuchMethodException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  public void abortRegionServer(int rs) {
    LOG.info("START abortRegionServer " + rs);
    getHBaseCluster().abortRegionServer(rs);
    LOG.info("DONE abortRegionServer " + rs + " Aborted");
  }

  public void startNewDatanode() throws IOException {
    LOG.info("START startNewDatanode");
    getDFSCluster().startDataNodes(getConfiguration(), 1, true, null, null);
    getDFSCluster().waitActive();
    LOG.info("DONE startNewDatanode");
  }

  public void stopCleanDataNode(int dn) {
    LOG.info("START stopCleanDataNode " + dn);
    getDFSCluster().stopDataNode(dn);
    LOG.info("DONE stopCleanDataNode " + dn + " Stopped");
  }

  public void stopDirtyDataNode(int dn) {
    LOG.info("START stopDirtyDataNode " + dn);
    getDFSCluster().getDataNodes().get(dn).shutdown();
    LOG.info("DONE stopDirtyDataNode " + dn + " Shut Downed");
  }

  public void stopDirtyDataNodeStopIPC(int dn) {
    LOG.info("START Shutdown Datanode and stop IPC" + dn);
    getDFSCluster().getDataNodes().get(dn).ipcServer.stop();
    getDFSCluster().getDataNodes().get(dn).shutdown();
    LOG.info("DONE stopDirtyDataNodeStopIPC " + dn + " shutdown and IPC stopped");
  }

  public void moveTableTo(String tableName, int rs) throws IOException {
    moveTableTo(tableName, rs, rs);
  }

  public void moveRegion(HRegionLocation hrl, int rs) throws IOException {
    LOG.info("START moveRegion " + hrl.getRegionInfo() + " to rs " + rs);
    String nameSrcServ = hrl.getHostnamePort();
    String nameDestServ = getHBaseCluster().getRegionServer(rs).getServerName().getHostAndPort();

    LOG.info("Region " + hrl.getRegionInfo().getEncodedName() + " was on " + nameSrcServ + " target is " + nameDestServ);
    if (!nameSrcServ.equals(nameDestServ)) {
      getHBaseAdmin().move(hrl.getRegionInfo().getEncodedNameAsBytes(),
          getHBaseCluster().getRegionServer(rs).getServerName().toString().getBytes());

      while (getHBaseCluster().getRegionServer(rs).getOnlineRegion(hrl.getRegionInfo().getRegionName()) == null ||
          !getHBaseCluster().getRegionServer(rs).getOnlineRegion(hrl.getRegionInfo().getRegionName()).isAvailable()) {
        Threads.sleep(1);
      }
    }
    LOG.info("DONE moveRegion");
  }


  public void moveTableTo(String tableName, int minRS, int maxRS) throws IOException {
    LOG.info("START: moveTableTo " + tableName + " to RS " + minRS + " to " + maxRS);

    HTable toMove = new HTable(getConfiguration(), tableName);
    int curRS = minRS;

    if (toMove.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW) == null) {
      LOG.info("No regions in table " + tableName);
    } else {
      for (HRegionLocation hrl :
          toMove.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)) {
        moveRegion(hrl, curRS);
        if (++curRS > maxRS) {
          curRS = minRS;
        }
      }
    }

    toMove.close();
    LOG.info("DONE: moveTableTo " + tableName + " to RS " + minRS + " to " + maxRS);
  }

  public void breakCache(int fakeRS) throws Exception {
    LOG.info("START breakCache " + fakeRS);
    HConnection hci = testTable.getConnection();

    Method updateCachedLocation = hci.getClass().getDeclaredMethod("updateCachedLocation",
        HRegionLocation.class, String.class, Integer.class);
    updateCachedLocation.setAccessible(true);

    int fakePort = getHBaseCluster().getRegionServer(fakeRS).getRpcServer().getListenerAddress().getPort();
    String fakeHostname = getHBaseCluster().getRegionServer(fakeRS).getRpcServer().getListenerAddress().getHostName();
    for (HRegionLocation hrl : testTable.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)) {
      LOG.info("Breaking cache by setting " + fakeHostname + ":" + fakePort + " for region " + hrl);
      updateCachedLocation.invoke(hrl, fakeHostname, fakePort);
    }
    LOG.info("DONE breakCache " + fakeRS);
  }

  public void createTableWithRegionsOnRS(int nRegions, int rs) throws IOException {
    createTable(nRegions, rs, rs);
  }

  public void createTable(int nRegions, int minRS, int maxRS) throws IOException {
    LOG.info("START createTable " + Bytes.toString(rb));
    testTable = super.createTable(rb, new byte[][]{rb}, 1, "0000000000".getBytes(),
        "9999999999".getBytes(), nRegions);
    moveTableTo(Bytes.toString(testTable.getTableName()), minRS, maxRS);
    LOG.info("DONE createTable " + Bytes.toString(rb));
  }


  public void createTableOnRS(int rs) throws IOException {
    createTableWithRegionsOnRS(1, rs);
  }


  public byte[] getTestTableName() {
    return testTable.getTableName();
  }

  public String getTestTableNameToString() {
    return Bytes.toString(getTestTableName());
  }


  public HTable getTestTable() {
    return testTable;
  }

  private ArrayList<byte[]> allPuts = new ArrayList<byte[]>();
  private byte[][] keys = null;
  private int curReg = 0;

  public void testAllPuts() throws IOException {
    for (byte[] allPut : allPuts) {
      Get g = new Get(allPut);
      Assert.assertArrayEquals(testTable.get(g).getRow(), allPut);
    }
  }

  public void stopCleanCluster() throws Exception {
    for (ServerSocket ss : portsTaken) {
      ss.close();
    }

    if (testTable != null) {
      testTable.close();
    }

    super.shutdownMiniCluster();
  }

  public class TestPuts {
    private byte[] incBytes(byte[] ba) {
      byte[] baC = ba.clone();
      for (int i = baC.length - 1; i > 0; i--) {
        if (baC[i] < 254) {
          baC[i]++;
          break;
        } else {
          baC[i] = 0;
        }
      }
      return baC;
    }

    private ArrayList<byte[]> pastPuts = new ArrayList<byte[]>();

    public TestPuts(int nbPut) throws Exception {
      if (keys == null || keys.length != testTable.getStartKeys().length) {
        keys = testTable.getStartKeys();
        curReg = 0;
      }

      ArrayList<Put> puts = new ArrayList<Put>();

      for (int i = 0; i < nbPut; i++) {
        byte[] startKey = keys[curReg];
        if (startKey == null || startKey.length == 0) {
          startKey = new byte[4];
        }
        keys[curReg] = incBytes(startKey);
        Put p = new Put(keys[curReg]);
        p.add(rb, rb, rb);
        puts.add(p);
        pastPuts.add(keys[curReg]);
        allPuts.add(keys[curReg]);
        curReg = ++curReg % keys.length;
      }
      testTable.put(puts);
    }

    public void checkPuts() throws Exception {
      for (byte[] pastPut : pastPuts) {
        Get g = new Get(pastPut);
        Assert.assertArrayEquals(testTable.get(g).getRow(), pastPut);
      }
    }
  }

  public void flushSynchronous(int RS) throws Exception {
    LOG.info("START flushSynchronous " + Bytes.toString(rb));
    getHBaseAdmin().flush(testTable.getTableName());

    Field cacheFlusher = HRegionServer.class.getDeclaredField("cacheFlusher");
    cacheFlusher.setAccessible(true);

    Object oCF = cacheFlusher.get(getHBaseCluster().getRegionServer(RS));

    Method meth = oCF.getClass().getMethod("getFlushQueueSize");
    meth.setAccessible(true);

    while ((Integer) meth.invoke(oCF) > 0) {
      Thread.sleep(1);
    }
    LOG.info("DONE flushSynchronous " + Bytes.toString(rb));
  }

  public void roll(int rs) throws IOException {
    getHBaseAdmin().rollHLogWriter(
        getHBaseCluster().getRegionServer(rs).getServerName().toString());
  }

  public HRegionInfo createRegion() {
    return new HRegionInfo(getTestTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
  }

  public DatanodeInfo[] getPipeline(HLog log) throws Exception {

    DistributedFileSystem dfs = (DistributedFileSystem) org.apache.hadoop.hdfs.DistributedFileSystem.get(super.getConfiguration());
    Method getPipeline = null;
    DFSClient cli = dfs.getClient();

    Field hdfs_out = HLog.class.getDeclaredField("hdfs_out");
    hdfs_out.setAccessible(true);
    FSDataOutputStream ho = (FSDataOutputStream) hdfs_out.get(log);

    DatanodeInfo[] datanodeInfos = null;

    getPipeline = ho.getWrappedStream().getClass().getDeclaredMethod("getPipeline");
    getPipeline.setAccessible(true);
    datanodeInfos = (DatanodeInfo[]) getPipeline.invoke(ho.getWrappedStream());

    return datanodeInfos;
  }

  public void stopDirtyDataNodeXOfPipelineAndTakePorts(int DN, HLog log) throws Exception {
    DatanodeInfo[] datanodeInfos = getPipeline(log);
                                        /*
    int i;
    for (i = 0;
         !datanodeInfos[DN - 1].getName().
             equals(getDFSCluster().getDataNodes().get(i).getSelfAddr().getAddress().getHostAddress() + ":" +
        getDFSCluster().getDataNodes().get(i).getSelfAddr().getPort()); i++)
      ;

    stopDirtyDataNodeTakePorts(i);            */
  }

  private void writeToWAL(int nbCol, WALEdit cols) {
    LOG.info("Start writeToWAL");
    // Write columns named 1, 2, 3, etc. and then values of single byte
    // 1, 2, 3...
    long timestamp = System.currentTimeMillis();
    LOG.info("Add column " + nbCol);
    cols.add(new KeyValue("row".getBytes(), Bytes.toBytes("column"), Bytes.toBytes(Integer.toString(nbCol)),
        timestamp, new byte[]{(byte) (nbCol + '0')}));
    LOG.info("Column " + nbCol + " added");
    LOG.info("writeToWAL finished");
  }

  private void flushRegion(byte[] RegionName, HLog log) throws IOException {
    LOG.info("Flush region " + RegionName);
    long logSeqId = log.startCacheFlush(RegionName);
    log.completeCacheFlush(RegionName, getTestTableName(), logSeqId, false);
    LOG.info("Region " + RegionName + " flushed");
  }

  private Path writeToLog(WALEdit cols, HLog log, HRegionInfo hri) throws Exception {
    LOG.info("Start writeToLog");

    // Create table
    HTableDescriptor htd = new HTableDescriptor();
    // Create family
    htd.addFamily(new HColumnDescriptor("column"));
    // Append to log
    log.append(hri, getTestTableName(), cols, System.currentTimeMillis(), htd);
    // You need to do a cache flush !
    flushRegion(hri.getEncodedNameAsBytes(), log);

    LOG.info("writeToLog finished");
    return getLogPath(log);
  }

  private Path getLogPath(HLog log) throws Exception {
    Method computeFilename = log.getClass().getDeclaredMethod("computeFilename");
    computeFilename.setAccessible(true);
    return (Path) computeFilename.invoke(log);
  }

  public Path createHLog(HRegionInfo hri, int nbDNtoKill) throws Exception {
    LOG.info("Start createHLog");

    LOG.info("Create HLog");
    FileSystem fs = getDFSCluster().getFileSystem();
    Path hbaseDir = createRootDir();
    Path oldLogDir = new Path(hbaseDir, ".oldlogs");
    Path dir = new Path(hbaseDir, "tests");
    HLog log = new HLog(fs, dir, oldLogDir, getConfiguration());
    LOG.info("Hlog Created");

    ServerSocket[][] sock = new ServerSocket[nbDNtoKill][];
    Path logPath = null;


    for (int i = 0; i <= nbDNtoKill; i++) {
      LOG.info("Create WALEdit");
      WALEdit cols = new WALEdit();
      LOG.info("WALEdit Created");
      writeToWAL(i, cols);

      // To allow write log
      // rollWriter(log);

      long start = System.currentTimeMillis();
      writeToLog(cols, log, hri);
      long stop = System.currentTimeMillis();
      long res = stop - start;
      LOG.info("Time to write HLog with " + i + " Datanodes killed : " + res + " ms");

      logPath = getLogPath(log);

      if (i < nbDNtoKill) {
        startNewDatanode();
        // always kill the second DN of pipeline
        stopDirtyDataNodeXOfPipelineAndTakePorts(2, log);
      }
    }


    freePorts();

    LOG.info("Close HLog");
    log.close();
    LOG.info("HLog closed");

    LOG.info("createHLog Finished");
    return logPath;
  }

  private void checkHLogValues(HLog.Entry entry, int nbCol, HRegionInfo hri) {
    LOG.info("Start checkValues");
    LOG.info("Verify if number of columns is ok");
    Assert.assertEquals(nbCol, entry.getEdit().size());

    int idx = 0;

    for (KeyValue val : entry.getEdit().getKeyValues()) {
      LOG.info("Check value " + idx);
      Assert.assertTrue(Bytes.equals(hri.getEncodedNameAsBytes(),
          entry.getKey().getEncodedRegionName()));
      Assert.assertTrue(Bytes.equals(getTestTableName(), entry.getKey().getTablename()));
      Assert.assertTrue(Bytes.equals("row".getBytes(), val.getRow()));
      Assert.assertEquals((byte) (idx + '0'), val.getValue()[0]);
      System.out.println(entry.getKey() + " " + val);
      LOG.info("Value " + idx + " checked");
      idx++;
    }
    LOG.info("checkValues finished");
  }

  private void checkHLogMetaValues(HLog.Entry entry, HRegionInfo hri) throws Exception {
    LOG.info("Start checkMeta");
    LOG.info("Verify if number of columns is ok");
    Assert.assertEquals(1, entry.getEdit().size());

    // Protected field
    Field METAROW = HLog.class.getDeclaredField("METAROW");
    METAROW.setAccessible(true);
    Field COMPLETE_CACHE_FLUSH = HLog.class.getDeclaredField("COMPLETE_CACHE_FLUSH");
    COMPLETE_CACHE_FLUSH.setAccessible(true);

    int idx = 0;

    for (KeyValue val : entry.getEdit().getKeyValues()) {
      LOG.info("Check value " + idx);
      Assert.assertTrue(Bytes.equals(hri.getEncodedNameAsBytes(),
          entry.getKey().getEncodedRegionName()));
      Assert.assertTrue(Bytes.equals(getTestTableName(), entry.getKey().getTablename()));
      Assert.assertTrue(Bytes.equals((byte[]) METAROW.get(HLog.class), val.getRow()));
      Assert.assertTrue(Bytes.equals(HLog.METAFAMILY, val.getFamily()));
      Assert.assertEquals(0, Bytes.compareTo((byte[]) COMPLETE_CACHE_FLUSH.get(HLog.class), val.getValue()));
      System.out.println(entry.getKey() + " " + val);
      LOG.info("Value " + idx + " checked");
      idx++;
    }
    LOG.info("checkMeta finished");
  }

  public void checkHLog(int nbCol, Path filename, HRegionInfo hri) throws Exception {
    LOG.info("START checkHLog");

    // Now open a reader on the log
    HLog.Reader reader = HLog.getReader(getDFSCluster().getFileSystem(), filename, getConfiguration());
    HLog.Entry entry = reader.next();

    checkHLogValues(entry, nbCol, hri);

    // Get next row... the meta flushed row.
    entry = reader.next();
    checkHLogMetaValues(entry, hri);
    reader.close();

    LOG.info("DONE checkHLog");
  }

  public void cleanTableLocationCache() throws Exception {
    HConnection hci = testTable.getConnection();
    Method delete = hci.getClass().getDeclaredMethod("deleteCachedLocation", byte[].class, byte[].class);
    delete.setAccessible(true);
    for (byte[] b : testTable.getStartKeys()) {
      delete.invoke(hci, testTable.getTableName(), b);
    }
    //hci.clearRegionCache();
  }

  public void startClusterSynchronous(int nDN, int nRS) throws Exception {
    updateConf(nDN);

    String[] dns = new String[nDN];
    for (int i = 0; i < nDN; i++) {
      dns[i] = "datanode_" + i;
    }
    super.startMiniCluster(1, nRS, dns);

    getHBaseCluster().waitForActiveAndReadyMaster();
    while (getHBaseCluster().getLiveRegionServerThreads().size() < nRS) {
      Threads.sleep(1);
    }
  }

  private void updateConf(int nDN) {
    Configuration conf = super.getConfiguration();
    conf.setInt("dfs.replication", nDN >= 3 ? 3 : nDN);
    conf.setInt("hbase.client.retries.number", 20);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing

    conf.setBoolean("dfs.support.append", true);

    // We want hdfs to retries multiple times, if not the new block allocation could fail
    conf.setInt("dfs.client.block.write.retries", 10);
  }
}
