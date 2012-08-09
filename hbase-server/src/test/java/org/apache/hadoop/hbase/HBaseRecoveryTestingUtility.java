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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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

  public int[] getRSNoRootAndNoMeta(int nbRS) {
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
    LOG.info("DONE stopCleanRegionServer " + rs + " Killed");
  }

  public void stopDirtyRegionServer(int rsPos) throws Exception {
    HRegionServer rs = getHBaseCluster().getRegionServer(rsPos);
    LOG.info("START stopDirtyRegionServer Region Server " + rsPos + " " + rs.getServerName());
    LOG.info("Number of live regions " + rs.getCopyOfOnlineRegionsSortedBySize().size());
    for (HRegion r : rs.getCopyOfOnlineRegionsSortedBySize().values()) {
      LOG.info("Closing server with region " + r.getRegionNameAsString());
    }

    Method kill = HRegionServer.class.getDeclaredMethod("kill");
    kill.setAccessible(true);
    kill.invoke(rs);
    while (rs.isOnline() || !rs.isStopped()){
      Thread.sleep(1);
    }

    LOG.info("DONE stopDirtyRegionServer " + rsPos + " Killed");
  }

  private ArrayList<ServerSocket> portsTaken = new ArrayList<ServerSocket>();

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
        HRegionLocation.class, String.class, Integer.class );
    updateCachedLocation.setAccessible(true);

    int fakePort = getHBaseCluster().getRegionServer(fakeRS).getRpcServer().getListenerAddress().getPort();
    String fakeHostname = getHBaseCluster().getRegionServer(fakeRS).getRpcServer().getListenerAddress().getHostName();
    for (HRegionLocation hrl : testTable.getRegionsInRange(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)) {
      LOG.info("Breaking cache by setting " + fakeHostname + ":" + fakePort + " for region " + hrl);
      updateCachedLocation.invoke(hrl, fakeHostname, fakePort);
    }
    LOG.info("DONE breakCache " + fakeRS);
  }

  public void createTable(int nRegions, int rs) throws IOException {
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
    createTable(1, rs);
  }


  public byte[] getTestTableName() {
    return testTable.getTableName();
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
    for (ServerSocket ss: portsTaken){
      ss.close();
    }

    if (testTable != null){
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
    LOG.info("flush");
    getHBaseAdmin().flush(testTable.getTableName());

    Field cacheFlusher = HRegionServer.class.getDeclaredField("cacheFlusher");
    cacheFlusher.setAccessible(true);

    Object oCF = cacheFlusher.get(getHBaseCluster().getRegionServer(RS));

    Method meth = oCF.getClass().getMethod("getFlushQueueSize");
    meth.setAccessible(true);

    while ((Integer) meth.invoke(oCF) > 0) {
      Thread.sleep(1);
    }
  }

  public void roll(int rs) throws IOException {
    getHBaseAdmin().rollHLogWriter(
        getHBaseCluster().getRegionServer(rs).getServerName().toString());
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
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    conf.setInt("dfs.replication", nDN >= 3 ? 3 : nDN);
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt("hbase.client.retries.number", 20);
    conf.setInt("hbase.regionserver.optionallogflushinterval", 0);
    conf.setInt("hbase.hstore.compactionThreshold", 100000);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);
    conf.setFloat(HConstants.LOAD_BALANCER_SLOP_KEY, (float) 100.0); // no load balancing
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_SPLITTING_KEY, true);
    // Make block sizes small.
    //conf.setInt("dfs.blocksize", 1024 * 1024);
    // needed for testAppendClose()
    conf.setBoolean("dfs.support.append", true);

    // We want hdfs to retries multiple times, if not the new block allocation could fail
    conf.setInt("dfs.client.block.write.retries", 10);
    // quicker heartbeat interval for faster DN death notification
    //conf.setInt("heartbeat.recheck.interval", 5000);
    //conf.setInt("dfs.heartbeat.interval", 1);
    //conf.setInt("dfs.socket.timeout", 5000);
    // faster failover with cluster.shutdown();fs.close() idiom
    //conf.setInt("ipc.client.connect.max.retries", 1);
    //conf.setInt("dfs.client.block.recovery.retries", 1);
    //conf.setInt("ipc.client.connection.maxidletime", 500);
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY, SampleRegionWALObserver.class.getName());
  }
}
