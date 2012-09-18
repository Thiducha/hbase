/**
 *
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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.DeserializationException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.handler.ClosedRegionHandler;
import org.apache.hadoop.hbase.master.handler.DisableTableHandler;
import org.apache.hadoop.hbase.master.handler.EnableTableHandler;
import org.apache.hadoop.hbase.master.handler.OpenedRegionHandler;
import org.apache.hadoop.hbase.master.handler.ServerShutdownHandler;
import org.apache.hadoop.hbase.master.handler.SplitRegionHandler;
import org.apache.hadoop.hbase.master.metrics.MasterMetrics;
import org.apache.hadoop.hbase.regionserver.RegionAlreadyInTransitionException;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.KeyLocker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.RootRegionTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKTable;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

/**
 * Manages and performs region assignment.
 * <p>
 * Monitors ZooKeeper for events related to regions in transition.
 * <p>
 * Handles existing regions in transition during master failover.
 */
@InterfaceAudience.Private
public class AssignmentManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

  public static final ServerName HBCK_CODE_SERVERNAME = new ServerName(HConstants.HBCK_CODE_NAME,
      -1, -1L);

  protected Server master;

  private ServerManager serverManager;

  private CatalogTracker catalogTracker;

  private TimeoutMonitor timeoutMonitor;

  private TimerUpdater timerUpdater;

  private LoadBalancer balancer;

  final private KeyLocker<String> locker = new KeyLocker<String>();

  /**
   * Used for assignment only.  TODO: revisit the assign lock scheme
   */
  final private KeyLocker<String> assignLocker = new KeyLocker<String>();

  /**
   * Map of regions to reopen after the schema of a table is changed. Key -
   * encoded region name, value - HRegionInfo
   */
  private final Map <String, HRegionInfo> regionsToReopen;

  /*
   * Maximum times we recurse an assignment.  See below in {@link #assign()}.
   */
  private final int maximumAssignmentAttempts;

  /** Plans for region movement. Key is the encoded version of a region name*/
  // TODO: When do plans get cleaned out?  Ever? In server open and in server
  // shutdown processing -- St.Ack
  // All access to this Map must be synchronized.
  final NavigableMap<String, RegionPlan> regionPlans =
    new TreeMap<String, RegionPlan>();

  private final ZKTable zkTable;

  // store all the table names in disabling state
  Set<String> disablingTables = new HashSet<String>();
  // store all the enabling state tablenames.
  Set<String> enablingTables = new HashSet<String>();

  /**
   * Contains the server which need to update timer, these servers will be
   * handled by {@link TimerUpdater}
   */
  private final ConcurrentSkipListSet<ServerName> serversInUpdatingTimer =
    new ConcurrentSkipListSet<ServerName>();

  private final ExecutorService executorService;

  //Thread pool executor service for timeout monitor
  private java.util.concurrent.ExecutorService threadPoolExecutorService;

  private List<EventType> ignoreStatesRSOffline = Arrays.asList(new EventType[]{
      EventType.RS_ZK_REGION_FAILED_OPEN, EventType.RS_ZK_REGION_CLOSED });

  /**
   * Set when we are doing master failover processing; cleared when failover
   * completes.
   */
  private volatile boolean failover = false;

  // Set holding all the regions which got processed while RIT was not
  // populated during master failover.
  private Map<String, HRegionInfo> failoverProcessedRegions =
    new HashMap<String, HRegionInfo>();

   // metrics instance to send metrics for RITs
   MasterMetrics masterMetrics;

   private final RegionStates regionStates;

  /**
   * Constructs a new assignment manager.
   *
   * @param master
   * @param serverManager
   * @param catalogTracker
   * @param service
   * @throws KeeperException
   * @throws IOException
   */
  public AssignmentManager(Server master, ServerManager serverManager,
      CatalogTracker catalogTracker, final LoadBalancer balancer,
      final ExecutorService service, MasterMetrics metrics) throws KeeperException, IOException {
    super(master.getZooKeeper());
    this.master = master;
    this.serverManager = serverManager;
    this.catalogTracker = catalogTracker;
    this.executorService = service;
    this.regionsToReopen = Collections.synchronizedMap
                           (new HashMap<String, HRegionInfo> ());
    Configuration conf = master.getConfiguration();
    this.timeoutMonitor = new TimeoutMonitor(
      conf.getInt("hbase.master.assignment.timeoutmonitor.period", 10000),
      master, serverManager,
      conf.getInt("hbase.master.assignment.timeoutmonitor.timeout", 1800000));
    this.timerUpdater = new TimerUpdater(conf.getInt(
        "hbase.master.assignment.timerupdater.period", 10000), master);
    Threads.setDaemonThreadRunning(timerUpdater.getThread(),
        master.getServerName() + ".timerUpdater");
    this.zkTable = new ZKTable(this.master.getZooKeeper());
    this.maximumAssignmentAttempts =
      this.master.getConfiguration().getInt("hbase.assignment.maximum.attempts", 10);
    this.balancer = balancer;
    this.threadPoolExecutorService = Executors.newCachedThreadPool();
    this.masterMetrics = metrics;// can be null only with tests.
    this.regionStates = new RegionStates(master, serverManager);
  }

  void startTimeOutMonitor() {
    Threads.setDaemonThreadRunning(timeoutMonitor.getThread(), master.getServerName()
        + ".timeoutMonitor");
  }

  /**
   * @return Instance of ZKTable.
   */
  public ZKTable getZKTable() {
    // These are 'expensive' to make involving trip to zk ensemble so allow
    // sharing.
    return this.zkTable;
  }

  /**
   * This SHOULD not be public. It is public now
   * because of some unit tests.
   *
   * TODO: make it package private and keep RegionStates in the master package
   */
  public RegionStates getRegionStates() {
    return regionStates;
  }

  public RegionPlan getRegionReopenPlan(HRegionInfo hri) {
    return new RegionPlan(hri, null, regionStates.getRegionServerOfRegion(hri));
  }

  /**
   * Add a regionPlan for the specified region.
   * @param encodedName
   * @param plan
   */
  public void addPlan(String encodedName, RegionPlan plan) {
    synchronized (regionPlans) {
      regionPlans.put(encodedName, plan);
    }
  }

  /**
   * Add a map of region plans.
   */
  public void addPlans(Map<String, RegionPlan> plans) {
    synchronized (regionPlans) {
      regionPlans.putAll(plans);
    }
  }

  /**
   * Set the list of regions that will be reopened
   * because of an update in table schema
   *
   * @param regions
   *          list of regions that should be tracked for reopen
   */
  public void setRegionsToReopen(List <HRegionInfo> regions) {
    for(HRegionInfo hri : regions) {
      regionsToReopen.put(hri.getEncodedName(), hri);
    }
  }

  /**
   * Used by the client to identify if all regions have the schema updates
   *
   * @param tableName
   * @return Pair indicating the status of the alter command
   * @throws IOException
   */
  public Pair<Integer, Integer> getReopenStatus(byte[] tableName)
  throws IOException {
    List <HRegionInfo> hris =
      MetaReader.getTableRegions(this.master.getCatalogTracker(), tableName);
    Integer pending = 0;
    for (HRegionInfo hri : hris) {
      String name = hri.getEncodedName();
      // no lock concurrent access ok: sequential consistency respected.
      if (regionsToReopen.containsKey(name)
          || regionStates.isRegionInTransition(name)) {
        pending++;
      }
    }
    return new Pair<Integer, Integer>(pending, hris.size());
  }
  /**
   * Reset all unassigned znodes.  Called on startup of master.
   * Call {@link #assignAllUserRegions()} after root and meta have been assigned.
   * @throws IOException
   * @throws KeeperException
   */
  void cleanoutUnassigned() throws IOException, KeeperException {
    // Cleanup any existing ZK nodes and start watching
    ZKAssign.deleteAllNodes(watcher);
    ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
      this.watcher.assignmentZNode);
  }

  /**
   * Called on startup.
   * Figures whether a fresh cluster start of we are joining extant running cluster.
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  void joinCluster() throws IOException,
      KeeperException, InterruptedException {
    // Concurrency note: In the below the accesses on regionsInTransition are
    // outside of a synchronization block where usually all accesses to RIT are
    // synchronized.  The presumption is that in this case it is safe since this
    // method is being played by a single thread on startup.

    // TODO: Regions that have a null location and are not in regionsInTransitions
    // need to be handled.

    // Scan META to build list of existing regions, servers, and assignment
    // Returns servers who have not checked in (assumed dead) and their regions
    Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers = rebuildUserRegions();

    // This method will assign all user regions if a clean server startup or
    // it will reconstitute master state and cleanup any leftovers from
    // previous master process.
    processDeadServersAndRegionsInTransition(deadServers);

    // Recover the tables that were not fully moved to DISABLED state.
    // These tables are in DISABLING state when the master restarted/switched.
    boolean isWatcherCreated = recoverTableInDisablingState(this.disablingTables);
    recoverTableInEnablingState(this.enablingTables, isWatcherCreated);
  }

  /**
   * Process all regions that are in transition in zookeeper and also
   * processes the list of dead servers by scanning the META.
   * Used by master joining an cluster.  If we figure this is a clean cluster
   * startup, will assign all user regions.
   * @param deadServers
   *          Map of dead servers and their regions. Can be null.
   * @throws KeeperException
   * @throws IOException
   * @throws InterruptedException
   */
  void processDeadServersAndRegionsInTransition(
      final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers)
  throws KeeperException, IOException, InterruptedException {
    List<String> nodes = ZKUtil.listChildrenAndWatchForNewChildren(watcher,
      watcher.assignmentZNode);

    if (nodes == null) {
      String errorMessage = "Failed to get the children from ZK";
      master.abort(errorMessage, new IOException(errorMessage));
      return;
    }

    // Run through all regions.  If they are not assigned and not in RIT, then
    // its a clean cluster startup, else its a failover.
    Map<HRegionInfo, ServerName> regions = regionStates.getRegionAssignments();
    for (Map.Entry<HRegionInfo, ServerName> e: regions.entrySet()) {
      if (!e.getKey().isMetaTable() && e.getValue() != null) {
        LOG.debug("Found " + e + " out on cluster");
        this.failover = true;
        break;
      }
      if (nodes.contains(e.getKey().getEncodedName())) {
        LOG.debug("Found " + e.getKey().getRegionNameAsString() + " in RITs");
        // Could be a meta region.
        this.failover = true;
        break;
      }
    }

    // Remove regions in RIT, they are possibly being processed by
    // ServerShutdownHandler.
    // no lock concurrent access ok: some threads may be adding/removing items but its java-valid
    nodes.removeAll(regionStates.getRegionsInTransition().keySet());

    // If some dead servers are processed by ServerShutdownHandler, we shouldn't
    // assign all user regions( some would be assigned by
    // ServerShutdownHandler), consider it as a failover
    if (!this.serverManager.getDeadServers().isEmpty()) {
      this.failover = true;
    }

    // If we found user regions out on cluster, its a failover.
    if (this.failover) {
      LOG.info("Found regions out on cluster or in RIT; failover");
      // Process list of dead servers and regions in RIT.
      // See HBASE-4580 for more information.
      processDeadServersAndRecoverLostRegions(deadServers, nodes);
      this.failover = false;
      failoverProcessedRegions.clear();
    } else {
      // Fresh cluster startup.
      LOG.info("Clean cluster startup. Assigning userregions");
      cleanoutUnassigned();
      assignAllUserRegions();
    }
  }

  /**
   * If region is up in zk in transition, then do fixup and block and wait until
   * the region is assigned and out of transition.  Used on startup for
   * catalog regions.
   * @param hri Region to look for.
   * @return True if we processed a region in transition else false if region
   * was not up in zk in transition.
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransitionAndBlockUntilAssigned(final HRegionInfo hri)
  throws InterruptedException, KeeperException, IOException {
    boolean intransistion =
      processRegionInTransition(hri.getEncodedName(), hri, null);
    if (!intransistion) return intransistion;
    LOG.debug("Waiting on " + HRegionInfo.prettyPrint(hri.getEncodedName()));
    while (!this.master.isStopped() &&
      this.regionStates.isRegionInTransition(hri.getEncodedName())) {
      // We put a timeout because we may have the region getting in just between the test
      //  and the waitForUpdate
      this.regionStates.waitForUpdate(100);
    }
    return intransistion;
  }

  /**
   * Process failover of new master for region <code>encodedRegionName</code>
   * up in zookeeper.
   * @param encodedRegionName Region to process failover for.
   * @param regionInfo If null we'll go get it from meta table.
   * @param deadServers Can be null
   * @return True if we processed <code>regionInfo</code> as a RIT.
   * @throws KeeperException
   * @throws IOException
   */
  boolean processRegionInTransition(final String encodedRegionName,
      final HRegionInfo regionInfo, final Map<ServerName,List<Pair<HRegionInfo,Result>>> deadServers)
  throws KeeperException, IOException {
    Stat stat = new Stat();
    byte [] data = ZKAssign.getDataAndWatch(watcher, encodedRegionName, stat);
    if (data == null) return false;
    RegionTransition rt;
    try {
      rt = RegionTransition.parseFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed parse znode data", e);
      return false;
    }
    HRegionInfo hri = regionInfo;
    if (hri == null) {
      if ((hri = getHRegionInfo(rt.getRegionName())) == null) return false;
    }
    processRegionsInTransition(rt, hri, deadServers, stat.getVersion());
    return true;
  }

  void processRegionsInTransition(final RegionTransition rt, final HRegionInfo regionInfo,
      final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers, int expectedVersion)
  throws KeeperException {
    EventType et = rt.getEventType();
    // Get ServerName.  Could be null.
    ServerName sn = rt.getServerName();
    String encodedRegionName = regionInfo.getEncodedName();
    LOG.info("Processing region " + regionInfo.getRegionNameAsString() + " in state " + et);

    // We need a lock here to ensure that we will not put the same region twice
    // It has no reason to be a lock shared with the other operations.
    // We can do the lock on the region only, instead of a global lock: what we want to ensure
    // is that we don't have two threads working on the same region.
    Lock lock = locker.acquireLock(encodedRegionName);
    try {
      if (regionStates.isRegionInTransition(encodedRegionName)
          || failoverProcessedRegions.containsKey(encodedRegionName)) {
        // Just return
        return;
      }
      switch (et) {
      case M_ZK_REGION_CLOSING:
        // If zk node of the region was updated by a live server skip this
        // region and just add it into RIT.
        if (isOnDeadServer(regionInfo, deadServers) && !isServerOnline(sn)) {
          // If was on dead server, its closed now. Force to OFFLINE and this
          // will get it reassigned if appropriate
          forceOffline(regionInfo, rt);
        } else {
          // Just insert region into RIT.
          // If this never updates the timeout will trigger new assignment
          regionStates.updateRegionState(rt, RegionState.State.CLOSING);
        }
        failoverProcessedRegions.put(encodedRegionName, regionInfo);
        break;

      case RS_ZK_REGION_CLOSED:
      case RS_ZK_REGION_FAILED_OPEN:
        // Region is closed, insert into RIT and handle it
        addToRITandCallClose(regionInfo, RegionState.State.CLOSED, rt);
        failoverProcessedRegions.put(encodedRegionName, regionInfo);
        break;

      case M_ZK_REGION_OFFLINE:
        // If zk node of the region was updated by a live server skip this
        // region and just add it into RIT.
        if (isOnDeadServer(regionInfo, deadServers) && (sn == null || !isServerOnline(sn))) {
          // Region is offline, insert into RIT and handle it like a closed
          addToRITandCallClose(regionInfo, RegionState.State.OFFLINE, rt);
        } else if (sn != null && !isServerOnline(sn)) {
          // to handle cases where offline node is created but sendRegionOpen
          // RPC is not yet sent
          addToRITandCallClose(regionInfo, RegionState.State.OFFLINE, rt);
        } else {
          regionStates.updateRegionState(rt, RegionState.State.PENDING_OPEN);
        }
        failoverProcessedRegions.put(encodedRegionName, regionInfo);
        break;

      case RS_ZK_REGION_OPENING:
        // TODO: Could check if it was on deadServers.  If it was, then we could
        // do what happens in TimeoutMonitor when it sees this condition.
        // Just insert region into RIT
        // If this never updates the timeout will trigger new assignment
        if (regionInfo.isMetaTable()) {
          regionStates.updateRegionState(rt, RegionState.State.OPENING);
          // If ROOT or .META. table is waiting for timeout monitor to assign
          // it may take lot of time when the assignment.timeout.period is
          // the default value which may be very long.  We will not be able
          // to serve any request during this time.
          // So we will assign the ROOT and .META. region immediately.
          processOpeningState(regionInfo);
          break;
        } else if (deadServers.keySet().contains(sn)) {
          // if the region is found on a dead server, we can assign
          // it to a new RS. (HBASE-5882)
          processOpeningState(regionInfo);
          break;
        }
        regionStates.updateRegionState(rt, RegionState.State.OPENING);
        failoverProcessedRegions.put(encodedRegionName, regionInfo);
        break;

      case RS_ZK_REGION_OPENED:
        // Region is opened, insert into RIT and handle it
        regionStates.updateRegionState(rt, RegionState.State.OPEN);
        // sn could be null if this server is no longer online.  If
        // that is the case, just let this RIT timeout; it'll be assigned
        // to new server then.
        if (sn == null) {
          LOG.warn("Region in transition " + regionInfo.getEncodedName() +
            " references a null server; letting RIT timeout so will be " +
            "assigned elsewhere");
        } else if (!serverManager.isServerOnline(sn)
            && (isOnDeadServer(regionInfo, deadServers)
                || regionInfo.isMetaRegion() || regionInfo.isRootRegion())) {
          forceOffline(regionInfo, rt);
        } else {
          new OpenedRegionHandler(master, this, regionInfo, sn, expectedVersion).process();
        }
        failoverProcessedRegions.put(encodedRegionName, regionInfo);
        break;
      case RS_ZK_REGION_SPLITTING:
        LOG.debug("Processed region in state : " + et);
        break;
      case RS_ZK_REGION_SPLIT:
        LOG.debug("Processed region in state : " + et);
        break;
      default:
        throw new IllegalStateException("Received region in state :" + et + " is not valid");
      }
    } finally {
      lock.unlock();
    }
  }


  /**
   * Put the region <code>hri</code> into an offline state up in zk.
   *
   * You need to have lock on the region before calling this method.
   *
   * @param hri
   * @param oldRt
   * @throws KeeperException
   */
  private void forceOffline(final HRegionInfo hri, final RegionTransition oldRt)
  throws KeeperException {
    // If was on dead server, its closed now.  Force to OFFLINE and then
    // handle it like a close; this will get it reassigned if appropriate
    LOG.debug("RIT " + hri.getEncodedName() + " in state=" + oldRt.getEventType() +
      " was on deadserver; forcing offline");
    ZKAssign.createOrForceNodeOffline(this.watcher, hri,
      this.master.getServerName());
    addToRITandCallClose(hri, RegionState.State.OFFLINE, oldRt);
  }

  /**
   * Add to the in-memory copy of regions in transition and then call close
   * handler on passed region <code>hri</code>
   * @param hri
   * @param state
   * @param oldData
   */
  private void addToRITandCallClose(final HRegionInfo hri,
                                    final RegionState.State state, final RegionTransition oldData) {
    regionStates.updateRegionState(oldData, state);
    new ClosedRegionHandler(this.master, this, hri).process();
  }

  /**
   * When a region is closed, it should be removed from the regionsToReopen
   * @param hri HRegionInfo of the region which was closed
   */
  public void removeClosedRegion(HRegionInfo hri) {
    if (regionsToReopen.remove(hri.getEncodedName()) != null) {
      LOG.debug("Removed region from reopening regions because it was closed");
    }
  }

  /**
   * @param regionInfo
   * @param deadServers Map of deadServers and the regions they were carrying;
   * can be null.
   * @return True if the passed regionInfo in the passed map of deadServers?
   */
  private boolean isOnDeadServer(final HRegionInfo regionInfo,
      final Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers) {
    if (deadServers == null) return false;
    for (Map.Entry<ServerName, List<Pair<HRegionInfo, Result>>> deadServer:
        deadServers.entrySet()) {
      for (Pair<HRegionInfo, Result> e: deadServer.getValue()) {
        if (e.getFirst().equals(regionInfo)) return true;
      }
    }
    return false;
  }

  /**
   * Handles various states an unassigned node can be in.
   * <p>
   * Method is called when a state change is suspected for an unassigned node.
   * <p>
   * This deals with skipped transitions (we got a CLOSED but didn't see CLOSING
   * yet).
   * @param rt
   * @param expectedVersion
   */
  private void handleRegion(final RegionTransition rt, int expectedVersion) {
    HRegionInfo hri = null;
    if (rt == null) {
      LOG.warn("Unexpected NULL input " + rt);
      return;
    }
    final ServerName sn = rt.getServerName();
    if (sn == null) {
      LOG.warn("Null servername: " + rt);
      return;
    }
    // Check if this is a special HBCK transition
    if (sn.equals(HBCK_CODE_SERVERNAME)) {
      handleHBCK(rt);
      return;
    }
    final long createTime = rt.getCreateTime();
    final byte[] regionName = rt.getRegionName();
    String encodedName = HRegionInfo.encodeRegionName(regionName);
    String prettyPrintedRegionName = HRegionInfo.prettyPrint(encodedName);
    // Verify this is a known server
    if (!serverManager.isServerOnline(sn) &&
      !this.master.getServerName().equals(sn)
      && !ignoreStatesRSOffline.contains(rt.getEventType())) {
      LOG.warn("Attempted to handle region transition for server but " +
        "server is not online: " + prettyPrintedRegionName);
      return;
    }

    // We need a lock on the region as we could update it
    Lock lock = locker.acquireLock(encodedName);
    try {
      // Printing if the event was created a long time ago helps debugging
      boolean lateEvent = createTime < (System.currentTimeMillis() - 15000);
      RegionState regionState = regionStates.getRegionTransitionState(encodedName);
      LOG.debug("Handling transition=" + rt.getEventType() +
        ", server=" + sn + ", region=" +
        (prettyPrintedRegionName == null ? "null" : prettyPrintedRegionName) +
        (lateEvent ? ", which is more than 15 seconds late" : "") +
        ", current state from region state map =" + regionState);
      switch (rt.getEventType()) {
        case M_ZK_REGION_OFFLINE:
          // Nothing to do.
          break;

        case RS_ZK_REGION_SPLITTING:
          if (!isInStateForSplitting(regionState)) break;
          regionStates.updateRegionState(rt, RegionState.State.SPLITTING);
          break;

        case RS_ZK_REGION_SPLIT:
          // RegionState must be null, or SPLITTING or PENDING_CLOSE.
          if (!isInStateForSplitting(regionState)) break;
          // If null, add SPLITTING state before going to SPLIT
          if (regionState == null) {
            regionState = regionStates.updateRegionState(rt,
              RegionState.State.SPLITTING);

            String message = "Received SPLIT for region " + prettyPrintedRegionName +
              " from server " + sn;
            // If still null, it means we cannot find it and it was already processed
            if (regionState == null) {
              LOG.warn(message + " but it doesn't exist anymore," +
                  " probably already processed its split");
              break;
            }
            LOG.info(message +
                " but region was not first in SPLITTING state; continuing");
          }
          // Check it has daughters.
          byte [] payload = rt.getPayload();
          List<HRegionInfo> daughters = null;
          try {
            daughters = HRegionInfo.parseDelimitedFrom(payload, 0, payload.length);
          } catch (IOException e) {
            LOG.error("Dropped split! Failed reading split payload for " +
              prettyPrintedRegionName);
            break;
          }
          assert daughters.size() == 2;
          // Assert that we can get a serverinfo for this server.
          if (!this.serverManager.isServerOnline(sn)) {
            LOG.error("Dropped split! ServerName=" + sn + " unknown.");
            break;
          }
          // Run handler to do the rest of the SPLIT handling.
          this.executorService.submit(new SplitRegionHandler(master, this,
            regionState.getRegion(), sn, daughters));
          break;

        case M_ZK_REGION_CLOSING:
          hri = checkIfInFailover(regionState, encodedName, regionName);
          if (hri != null) {
            regionState = regionStates.updateRegionState(
              hri, RegionState.State.CLOSING, createTime, sn);
            failoverProcessedRegions.put(encodedName, hri);
            break;
          }
          // Should see CLOSING after we have asked it to CLOSE or additional
          // times after already being in state of CLOSING
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSING for region " + prettyPrintedRegionName +
              " from server " + sn + " but region was in " +
              " the state " + regionState + " and not " +
              "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Transition to CLOSING (or update stamp if already CLOSING)
          regionStates.updateRegionState(rt, RegionState.State.CLOSING);
          break;

        case RS_ZK_REGION_CLOSED:
          hri = checkIfInFailover(regionState, encodedName, regionName);
          if (hri != null) {
            regionState = regionStates.updateRegionState(
              hri, RegionState.State.CLOSED, createTime, sn);
            removeClosedRegion(regionState.getRegion());
            new ClosedRegionHandler(master, this, regionState.getRegion())
              .process();
            failoverProcessedRegions.put(encodedName, hri);
            break;
          }
          // Should see CLOSED after CLOSING but possible after PENDING_CLOSE
          if (regionState == null ||
              (!regionState.isPendingClose() && !regionState.isClosing())) {
            LOG.warn("Received CLOSED for region " + prettyPrintedRegionName +
                " from server " + sn + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_CLOSE or CLOSING states");
            return;
          }
          // Handle CLOSED by assigning elsewhere or stopping if a disable
          // If we got here all is good.  Need to update RegionState -- else
          // what follows will fail because not in expected state.
          regionStates.updateRegionState(rt, RegionState.State.CLOSED);
          removeClosedRegion(regionState.getRegion());
          this.executorService.submit(new ClosedRegionHandler(master,
            this, regionState.getRegion()));
          break;

        case RS_ZK_REGION_FAILED_OPEN:
          hri = checkIfInFailover(regionState, encodedName, regionName);
          if (hri != null) {
            regionState = regionStates.updateRegionState(
              hri, RegionState.State.CLOSED, createTime, sn);
            new ClosedRegionHandler(master, this, regionState.getRegion())
              .process();
            failoverProcessedRegions.put(encodedName, hri);
            break;
          }
          if (regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received FAILED_OPEN for region " + prettyPrintedRegionName +
                " from server " + sn + " but region was in " +
                " the state " + regionState + " and not in PENDING_OPEN or OPENING");
            return;
          }
          // Handle this the same as if it were opened and then closed.
          regionStates.updateRegionState(rt, RegionState.State.CLOSED);
          // When there are more than one region server a new RS is selected as the
          // destination and the same is updated in the regionplan. (HBASE-5546)
          getRegionPlan(regionState, sn, true);
          this.executorService.submit(new ClosedRegionHandler(master,
            this, regionState.getRegion()));
          break;

        case RS_ZK_REGION_OPENING:
          hri = checkIfInFailover(regionState, encodedName, regionName);
          if (hri != null) {
            regionState = regionStates.updateRegionState(
              hri, RegionState.State.OPENING, createTime, sn);
            failoverProcessedRegions.put(encodedName, hri);
            break;
          }
          // Should see OPENING after we have asked it to OPEN or additional
          // times after already being in state of OPENING
          if (regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENING for region " +
                prettyPrintedRegionName +
                " from server " + sn + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Transition to OPENING (or update stamp if already OPENING)
          regionStates.updateRegionState(rt, RegionState.State.OPENING);
          break;

        case RS_ZK_REGION_OPENED:
          hri = checkIfInFailover(regionState, encodedName, regionName);
          if (hri != null) {
            regionState = regionStates.updateRegionState(
              hri, RegionState.State.OPEN, createTime, sn);
            new OpenedRegionHandler(master, this, regionState.getRegion(), sn, expectedVersion).process();
            failoverProcessedRegions.put(encodedName, hri);
            break;
          }
          // Should see OPENED after OPENING but possible after PENDING_OPEN
          if (regionState == null ||
              (!regionState.isPendingOpen() && !regionState.isOpening())) {
            LOG.warn("Received OPENED for region " +
                prettyPrintedRegionName +
                " from server " + sn + " but region was in " +
                " the state " + regionState + " and not " +
                "in expected PENDING_OPEN or OPENING states");
            return;
          }
          // Handle OPENED by removing from transition and deleted zk node
          regionStates.updateRegionState(rt, RegionState.State.OPEN);
          this.executorService.submit(
            new OpenedRegionHandler(master, this, regionState.getRegion(), sn, expectedVersion));
          break;

        default:
          throw new IllegalStateException("Received event is not valid.");
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Checks whether the callback came while RIT was not yet populated during
   * master failover.
   * @param regionState
   * @param encodedName
   * @param data
   * @return hri
   */
  private HRegionInfo checkIfInFailover(RegionState regionState,
      String encodedName, final byte [] regionName) {
    if (regionState == null && this.failover &&
        (failoverProcessedRegions.containsKey(encodedName) == false ||
          failoverProcessedRegions.get(encodedName) == null)) {
      HRegionInfo hri = this.failoverProcessedRegions.get(encodedName);
      if (hri == null) hri = getHRegionInfo(regionName);
      return hri;
    }
    return null;
  }

  /**
   * Gets the HRegionInfo from the META table
   * @param  regionName
   * @return HRegionInfo hri for the region
   */
  private HRegionInfo getHRegionInfo(final byte [] regionName) {
    Pair<HRegionInfo, ServerName> p = null;
    try {
      p = MetaReader.getRegion(catalogTracker, regionName);
      if (p == null) return null;
      return p.getFirst();
    } catch (IOException e) {
      master.abort("Aborting because error occoured while reading " +
        Bytes.toStringBinary(regionName) + " from .META.", e);
      return null;
    }
  }

  /**
   * @return Returns true if this RegionState is splittable; i.e. the
   * RegionState is currently in splitting state or pending_close or
   * null (Anything else will return false). (Anything else will return false).
   */
  private boolean isInStateForSplitting(final RegionState rs) {
    if (rs == null) return true;
    if (rs.isSplitting()) return true;
    if (convertPendingCloseToSplitting(rs)) return true;
    LOG.warn("Dropped region split! Not in state good for SPLITTING; rs=" + rs);
    return false;
  }

  /**
   * If the passed regionState is in PENDING_CLOSE, clean up PENDING_CLOSE
   * state and convert it to SPLITTING instead.
   * This can happen in case where master wants to close a region at same time
   * a regionserver starts a split.  The split won.  Clean out old PENDING_CLOSE
   * state.
   * @param rs
   * @return True if we converted from PENDING_CLOSE to SPLITTING
   */
  private boolean convertPendingCloseToSplitting(final RegionState rs) {
    if (!rs.isPendingClose()) return false;
    LOG.debug("Converting PENDING_CLOSE to SPLITING; rs=" + rs);
    regionStates.updateRegionState(
      rs.getRegion(), RegionState.State.SPLITTING);
    // Clean up existing state.  Clear from region plans seems all we
    // have to do here by way of clean up of PENDING_CLOSE.
    clearRegionPlan(rs.getRegion());
    return true;
  }

  /**
   * Handle a ZK unassigned node transition triggered by HBCK repair tool.
   * <p>
   * This is handled in a separate code path because it breaks the normal rules.
   * @param rt
   */
  private void handleHBCK(RegionTransition rt) {
    String encodedName = HRegionInfo.encodeRegionName(rt.getRegionName());
    LOG.info("Handling HBCK triggered transition=" + rt.getEventType() +
      ", server=" + rt.getServerName() + ", region=" +
      HRegionInfo.prettyPrint(encodedName));
    RegionState regionState = regionStates.getRegionTransitionState(encodedName);
    switch (rt.getEventType()) {
      case M_ZK_REGION_OFFLINE:
        HRegionInfo regionInfo = null;
        if (regionState != null) {
          regionInfo = regionState.getRegion();
        } else {
          try {
            byte [] name = rt.getRegionName();
            Pair<HRegionInfo, ServerName> p = MetaReader.getRegion(catalogTracker, name);
            regionInfo = p.getFirst();
          } catch (IOException e) {
            LOG.info("Exception reading META doing HBCK repair operation", e);
            return;
          }
        }
        LOG.info("HBCK repair is triggering assignment of region=" +
            regionInfo.getRegionNameAsString());
        // trigger assign, node is already in OFFLINE so don't need to update ZK
        assign(regionInfo, false);
        break;

      default:
        LOG.warn("Received unexpected region state from HBCK: " + rt.toString());
        break;
    }

  }

  // ZooKeeper events

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING or CLOSING of a region by
   * creating an unassigned node.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeCreated(String path) {
    handleAssignmentEvent(path);
  }

  /**
   * Existing unassigned node has had data changed.
   *
   * <p>This happens when an RS transitions from OFFLINE to OPENING, or between
   * OPENING/OPENED and CLOSING/CLOSED.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further events</li>
   *   <li>Read and handle the state in the node</li>
   * </ol>
   */
  @Override
  public void nodeDataChanged(String path) {
    handleAssignmentEvent(path);
  }

  private void handleAssignmentEvent(final String path) {
    if (!path.startsWith(watcher.assignmentZNode)) return;
    try {
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataAndWatch(watcher, path, stat);
      if (data == null) return;
      RegionTransition rt = RegionTransition.parseFrom(data);
      handleRegion(rt, stat.getVersion());
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception reading unassigned node data", e);
    } catch (DeserializationException e) {
      master.abort("Unexpected exception deserializing node data", e);
    }
  }

  @Override
  public void nodeDeleted(final String path) {
    if (path.startsWith(this.watcher.assignmentZNode)) {
      String regionName = ZKAssign.getRegionName(this.master.getZooKeeper(), path);
      RegionState rs = regionStates.getRegionTransitionState(regionName);
      if (rs != null) {
        HRegionInfo regionInfo = rs.getRegion();
        if (rs.isSplit()) {
          LOG.debug("Ephemeral node deleted, regionserver crashed?, " +
            "clearing from RIT; rs=" + rs);
          regionOffline(rs.getRegion());
        } else {
          LOG.debug("The znode of region " + regionInfo.getRegionNameAsString()
              + " has been deleted.");
          if (rs.isOpened()) {
            ServerName serverName = rs.getServerName();
            regionOnline(regionInfo, serverName);
            LOG.info("The master has opened the region "
              + regionInfo.getRegionNameAsString() + " that was online on "
              + serverName);
            if (this.getZKTable().isDisablingOrDisabledTable(
                regionInfo.getTableNameAsString())) {
              LOG.debug("Opened region "
                  + regionInfo.getRegionNameAsString() + " but "
                  + "this table is disabled, triggering close of region");
              unassign(regionInfo);
            }
          }
        }
      }
    }
  }

  /**
   * New unassigned node has been created.
   *
   * <p>This happens when an RS begins the OPENING, SPLITTING or CLOSING of a
   * region by creating a znode.
   *
   * <p>When this happens we must:
   * <ol>
   *   <li>Watch the node for further children changed events</li>
   *   <li>Watch all new children for changed events</li>
   * </ol>
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.assignmentZNode)) {
      try {
        // Just make sure we see the changes for the new znodes
        ZKUtil.listChildrenAndWatchThem(watcher,
            watcher.assignmentZNode);
      } catch(KeeperException e) {
        master.abort("Unexpected ZK exception reading unassigned children", e);
      }
    }
  }

  /**
   * Marks the region as online.  Removes it from regions in transition and
   * updates the in-memory assignment information.
   * <p>
   * Used when a region has been successfully opened on a region server.
   * @param regionInfo
   * @param sn
   */
  void regionOnline(HRegionInfo regionInfo, ServerName sn) {
    if (!isServerOnline(sn)) {
      LOG.warn("A region was opened on a dead server, ServerName=" +
        sn.getServerName() + ", region=" + regionInfo.getEncodedName());
    }

    regionStates.regionOnline(regionInfo, sn);

    // Remove plan if one.
    clearRegionPlan(regionInfo);
    // Add the server to serversInUpdatingTimer
    addToServersInUpdatingTimer(sn);
  }

  /**
   * Add the server to the set serversInUpdatingTimer, then {@link TimerUpdater}
   * will update timers for this server in background
   * @param sn
   */
  private void addToServersInUpdatingTimer(final ServerName sn) {
    this.serversInUpdatingTimer.add(sn);
  }

  /**
   * Touch timers for all regions in transition that have the passed
   * <code>sn</code> in common.
   * Call this method whenever a server checks in.  Doing so helps the case where
   * a new regionserver has joined the cluster and its been given 1k regions to
   * open.  If this method is tickled every time the region reports in a
   * successful open then the 1k-th region won't be timed out just because its
   * sitting behind the open of 999 other regions.  This method is NOT used
   * as part of bulk assign -- there we have a different mechanism for extending
   * the regions in transition timer (we turn it off temporarily -- because
   * there is no regionplan involved when bulk assigning.
   * @param sn
   */
  private void updateTimers(final ServerName sn) {
    if (sn == null) return;

    // This loop could be expensive.
    // First make a copy of current regionPlan rather than hold sync while
    // looping because holding sync can cause deadlock.  Its ok in this loop
    // if the Map we're going against is a little stale
    List<Map.Entry<String, RegionPlan>> rps;
    synchronized(this.regionPlans) {
      rps = new ArrayList<Map.Entry<String, RegionPlan>>(regionPlans.entrySet());
    }

    for (Map.Entry<String, RegionPlan> e : rps) {
      if (e.getValue() != null && e.getKey() != null && sn.equals(e.getValue().getDestination())) {
        RegionState regionState = regionStates.getRegionTransitionState(e.getKey());
        if (regionState != null) {
          regionState.updateTimestampToNow();
        }
      }
    }
  }

  /**
   * Marks the region as offline.  Removes it from regions in transition and
   * removes in-memory assignment information.
   * <p>
   * Used when a region has been closed and should remain closed.
   * @param regionInfo
   */
  public void regionOffline(final HRegionInfo regionInfo) {
    regionStates.regionOffline(regionInfo);

    // remove the region plan as well just in case.
    clearRegionPlan(regionInfo);
  }

  public void offlineDisabledRegion(HRegionInfo regionInfo) {
    // Disabling so should not be reassigned, just delete the CLOSED node
    LOG.debug("Table being disabled so deleting ZK node and removing from " +
        "regions in transition, skipping assignment of region " +
          regionInfo.getRegionNameAsString());
    try {
      if (!ZKAssign.deleteClosedNode(watcher, regionInfo.getEncodedName())) {
        // Could also be in OFFLINE mode
        ZKAssign.deleteOfflineNode(watcher, regionInfo.getEncodedName());
      }
    } catch (KeeperException.NoNodeException nne) {
      LOG.debug("Tried to delete closed node for " + regionInfo + " but it " +
          "does not exist so just offlining");
    } catch (KeeperException e) {
      this.master.abort("Error deleting CLOSED node in ZK", e);
    }
    regionOffline(regionInfo);
  }

  // Assignment methods

  /**
   * Assigns the specified region.
   * <p>
   * If a RegionPlan is available with a valid destination then it will be used
   * to determine what server region is assigned to.  If no RegionPlan is
   * available, region will be assigned to a random available server.
   * <p>
   * Updates the RegionState and sends the OPEN RPC.
   * <p>
   * This will only succeed if the region is in transition and in a CLOSED or
   * OFFLINE state or not in transition (in-memory not zk), and of course, the
   * chosen server is up and running (It may have just crashed!).  If the
   * in-memory checks pass, the zk node is forced to OFFLINE before assigning.
   *
   * @param region server to be assigned
   * @param setOfflineInZK whether ZK node should be created/transitioned to an
   *                       OFFLINE state before assigning the region
   */
  public void assign(HRegionInfo region, boolean setOfflineInZK) {
    assign(region, setOfflineInZK, false);
  }

  public void assign(HRegionInfo region, boolean setOfflineInZK,
      boolean forceNewPlan) {
    assign(region, setOfflineInZK, forceNewPlan, false);
  }

  /**
   * @param region
   * @param setOfflineInZK
   * @param forceNewPlan
   * @param hijack True if new assignment is needed, false otherwise
   */
  public void assign(HRegionInfo region, boolean setOfflineInZK,
      boolean forceNewPlan, boolean hijack) {
    // If hijack is true do not call disableRegionIfInRIT as
    // we have not yet moved the znode to OFFLINE state.
    if (!hijack && isDisabledorDisablingRegionInRIT(region)) {
      return;
    }
    if (this.serverManager.isClusterShutdown()) {
      LOG.info("Cluster shutdown is set; skipping assign of " +
        region.getRegionNameAsString());
      return;
    }
    RegionState state = forceRegionStateToOffline(region,
        hijack);
    // TODO: we can't synchronized on state any more since it could
    // be an new instance.  We need to reconsider how to avoid
    // double/multiple assignments.
    // This is to prevent double assignments? Does it work?
    String encodedName = region.getEncodedName();
    Lock lock = assignLocker.acquireLock(encodedName);
    try {
      assign(region, state, setOfflineInZK, forceNewPlan, hijack);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Bulk assign regions to <code>destination</code>.
   * @param destination
   * @param regions Regions to assign.
   * @return true if successful
   */
  boolean assign(final ServerName destination,
      final List<HRegionInfo> regions) {
    if (regions.size() == 0) {
      return true;
    }
    LOG.debug("Bulk assigning " + regions.size() + " region(s) to " +
      destination.toString());

    List<RegionState> states = new ArrayList<RegionState>(regions.size());
    for (HRegionInfo region : regions) {
      states.add(forceRegionStateToOffline(region));
    }
    // Add region plans, so we can updateTimers when one region is opened so
    // that unnecessary timeout on RIT is reduced.
    Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>(regions.size());
    for (HRegionInfo region : regions) {
      plans.put(region.getEncodedName(), new RegionPlan(region, null,
          destination));
    }
    this.addPlans(plans);

    // Presumption is that only this thread will be updating the state at this
    // time; i.e. handlers on backend won't be trying to set it to OPEN, etc.
    AtomicInteger counter = new AtomicInteger(0);
    CreateUnassignedAsyncCallback cb =
      new CreateUnassignedAsyncCallback(regionStates, this.watcher, destination, counter);
    for (RegionState state: states) {
      if (!asyncSetOfflineInZooKeeper(state, cb, state)) {
        return false;
      }
    }
    // Wait until all unassigned nodes have been put up and watchers set.
    int total = regions.size();
    for (int oldCounter = 0; true;) {
      int count = counter.get();
      if (oldCounter != count) {
        LOG.info(destination.toString() + " unassigned znodes=" + count +
          " of total=" + total);
        oldCounter = count;
      }
      if (count == total) break;
      Threads.sleep(1);
    }
    // Move on to open regions.
    try {
      // Send OPEN RPC. If it fails on a IOE or RemoteException, the
      // TimeoutMonitor will pick up the pieces.
      long maxWaitTime = System.currentTimeMillis() +
        this.master.getConfiguration().
          getLong("hbase.regionserver.rpc.startup.waittime", 60000);
      while (!this.master.isStopped()) {
        try {
          List<RegionOpeningState> regionOpeningStateList = this.serverManager
              .sendRegionOpen(destination, regions);
          if (regionOpeningStateList == null) {
            // Failed getting RPC connection to this server
            return false;
          }
          for (int i = 0; i < regionOpeningStateList.size(); i++) {
            if (regionOpeningStateList.get(i) == RegionOpeningState.ALREADY_OPENED) {
              processAlreadyOpenedRegion(regions.get(i), destination);
            } else if (regionOpeningStateList.get(i) == RegionOpeningState.FAILED_OPENING) {
              // Failed opening this region, reassign it
              assign(regions.get(i), true, true);
            }
          }
          break;
        } catch (RemoteException e) {
          IOException decodedException = e.unwrapRemoteException();
          if (decodedException instanceof RegionServerStoppedException) {
            LOG.warn("The region server was shut down, ", decodedException);
            // No need to retry, the region server is a goner.
            return false;
          } else if (decodedException instanceof ServerNotRunningYetException) {
            // This is the one exception to retry.  For all else we should just fail
            // the startup.
            long now = System.currentTimeMillis();
            if (now > maxWaitTime) throw e;
            LOG.debug("Server is not yet up; waiting up to " +
                (maxWaitTime - now) + "ms", e);
            Thread.sleep(1000);
          }

          throw decodedException;
        }
      }
    } catch (IOException e) {
      // Can be a socket timeout, EOF, NoRouteToHost, etc
      LOG.info("Unable to communicate with the region server in order" +
          " to assign regions", e);
      return false;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Bulk assigning done for " + destination.toString());
    return true;
  }

  /**
   * Bulk assign regions to available servers if any with retry, else assign
   * region singly.
   *
   * @param regions all regions to assign
   * @param servers all available servers
   */
  public void assign(List<HRegionInfo> regions, List<ServerName> servers) {
    LOG.info("Quickly assigning " + regions.size() + " region(s) across "
        + servers.size() + " server(s)");
    Map<ServerName, List<HRegionInfo>> bulkPlan = balancer
        .roundRobinAssignment(regions, servers);
    if (bulkPlan == null || bulkPlan.isEmpty()) {
      LOG.info("Failed getting bulk plan, assigning region singly");
      for (HRegionInfo region : regions) {
        assign(region, true);
      }
      return;
    }
    Map<ServerName, List<HRegionInfo>> failedPlans = new HashMap<ServerName, List<HRegionInfo>>();
    for (Map.Entry<ServerName, List<HRegionInfo>> e : bulkPlan.entrySet()) {
      try {
        if (!assign(e.getKey(), e.getValue())) {
          failedPlans.put(e.getKey(), e.getValue());
        }
      } catch (Throwable t) {
        LOG.warn("Failed bulking assigning " + e.getValue().size()
            + " region(s) to " + e.getKey().getServerName()
            + ", and continue to bulk assign others", t);
        failedPlans.put(e.getKey(), e.getValue());
      }
    }
    if (!failedPlans.isEmpty()) {
      servers.removeAll(failedPlans.keySet());
      List<HRegionInfo> reassigningRegions = new ArrayList<HRegionInfo>();
      for (Map.Entry<ServerName, List<HRegionInfo>> e : failedPlans.entrySet()) {
        LOG.info("Failed assigning " + e.getValue().size()
            + " regions to server " + e.getKey() + ", reassigning them");
        reassigningRegions.addAll(e.getValue());
      }
      for (HRegionInfo region : reassigningRegions) {
        assign(region, true, true);
      }
    }
  }

  /**
   * Callback handler for create unassigned znodes used during bulk assign.
   */
  static class CreateUnassignedAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateUnassignedAsyncCallback.class);
    private final RegionStates regionStates;
    private final ZooKeeperWatcher zkw;
    private final ServerName destination;
    private final AtomicInteger counter;

    CreateUnassignedAsyncCallback(final RegionStates regionStates,
        final ZooKeeperWatcher zkw, final ServerName destination,
        final AtomicInteger counter) {
      this.regionStates = regionStates;
      this.zkw = zkw;
      this.destination = destination;
      this.counter = counter;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        // Thisis resultcode.  If non-zero, need to resubmit.
        LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
          "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
        this.zkw.abort("Connectionloss writing unassigned at " + path +
          ", rc=" + rc, null);
        return;
      }
      LOG.debug("rs=" + (RegionState)ctx + ", server=" + this.destination.toString());
      // Async exists to set a watcher so we'll get triggered when
      // unassigned node changes.
      this.zkw.getRecoverableZooKeeper().getZooKeeper().exists(path, this.zkw,
        new ExistsUnassignedAsyncCallback(regionStates, counter, destination), ctx);
    }
  }

  /**
   * Callback handler for the exists call that sets watcher on unassigned znodes.
   * Used during bulk assign on startup.
   */
  static class ExistsUnassignedAsyncCallback implements AsyncCallback.StatCallback {
    private final Log LOG = LogFactory.getLog(ExistsUnassignedAsyncCallback.class);
    private final RegionStates regionStates;
    private final AtomicInteger counter;
    private ServerName destination;

    ExistsUnassignedAsyncCallback(final RegionStates regionStates,
        final AtomicInteger counter, ServerName destination) {
      this.regionStates = regionStates;
      this.counter = counter;
      this.destination = destination;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc != 0) {
        // This is result code.  If non-zero, need to resubmit.
        LOG.warn("rc != 0 for " + path + " -- retryable connectionloss -- " +
          "FIX see http://wiki.apache.org/hadoop/ZooKeeper/FAQ#A2");
        return;
      }
      RegionState state = (RegionState)ctx;
      LOG.debug("rs=" + state);
      // Transition RegionState to PENDING_OPEN here in master; means we've
      // sent the open.  We're a little ahead of ourselves here since we've not
      // yet sent out the actual open but putting this state change after the
      // call to open risks our writing PENDING_OPEN after state has been moved
      // to OPENING by the regionserver.
      regionStates.updateRegionState(state.getRegion(),
        RegionState.State.PENDING_OPEN, System.currentTimeMillis(),
        destination);
      this.counter.addAndGet(1);
    }
  }

  /**
   * Sets regions {@link RegionState} to {@link RegionState.State#OFFLINE}.
   * @param region
   * @return Amended RegionState.
   */
  private RegionState forceRegionStateToOffline(final HRegionInfo region) {
    return forceRegionStateToOffline(region, false);
  }

  /**
   * Sets regions {@link RegionState} to {@link RegionState.State#OFFLINE}.
   * @param region
   * @param hijack
   * @return Amended RegionState.
   */
  private RegionState forceRegionStateToOffline(final HRegionInfo region,
      boolean hijack) {
    String encodedName = region.getEncodedName();

    Lock lock = locker.acquireLock(encodedName);
    try {
      RegionState state = regionStates.getRegionTransitionState(encodedName);
      if (state == null) {
        state = regionStates.updateRegionState(
          region, RegionState.State.OFFLINE);
      } else {
        // If we are reassigning the node do not force in-memory state to OFFLINE.
        // Based on the znode state we will decide if to change in-memory state to
        // OFFLINE or not. It will be done before setting znode to OFFLINE state.

        // We often get here with state == CLOSED because ClosedRegionHandler will
        // assign on its tail as part of the handling of a region close.
        if (!hijack) {
          LOG.debug("Forcing OFFLINE; was=" + state);
          state = regionStates.updateRegionState(
            region, RegionState.State.OFFLINE);
        }
      }
      return state;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Caller must hold lock on the passed <code>state</code> object.
   * @param state
   * @param setOfflineInZK
   * @param forceNewPlan
   * @param hijack
   */
  private void assign(final HRegionInfo region, final RegionState state,
      final boolean setOfflineInZK, final boolean forceNewPlan,
      boolean hijack) {
    boolean regionAlreadyInTransitionException = false;
    for (int i = 0; i < this.maximumAssignmentAttempts; i++) {
      int versionOfOfflineNode = -1;
      if (setOfflineInZK) {
        // get the version of the znode after setting it to OFFLINE.
        // versionOfOfflineNode will be -1 if the znode was not set to OFFLINE
        versionOfOfflineNode = setOfflineInZooKeeper(state, hijack,
            regionAlreadyInTransitionException);
        if (versionOfOfflineNode != -1) {
          if (isDisabledorDisablingRegionInRIT(region)) {
            return;
          }
          // In case of assignment from EnableTableHandler table state is ENABLING. Any how
          // EnableTableHandler will set ENABLED after assigning all the table regions. If we
          // try to set to ENABLED directly then client api may think table is enabled.
          // When we have a case such as all the regions are added directly into .META. and we call
          // assignRegion then we need to make the table ENABLED. Hence in such case the table
          // will not be in ENABLING or ENABLED state.
          String tableName = region.getTableNameAsString();
          if (!zkTable.isEnablingTable(tableName) && !zkTable.isEnabledTable(tableName)) {
            LOG.debug("Setting table " + tableName + " to ENABLED state.");
            setEnabledTable(region);
          }
        }
      }
      if (setOfflineInZK && versionOfOfflineNode == -1) {
        return;
      }
      if (this.master.isStopped()) {
        LOG.debug("Server stopped; skipping assign of " + state);
        return;
      }
      RegionPlan plan = getRegionPlan(state, forceNewPlan);
      if (plan == null) {
        LOG.debug("Unable to determine a plan to assign " + state);
        this.timeoutMonitor.setAllRegionServersOffline(true);
        return; // Should get reassigned later when RIT times out.
      }
      try {
        LOG.info("Assigning region " + state.getRegion().getRegionNameAsString() +
          " to " + plan.getDestination().toString());
        // Transition RegionState to PENDING_OPEN
        regionStates.updateRegionState(state.getRegion(),
          RegionState.State.PENDING_OPEN, System.currentTimeMillis(),
          plan.getDestination());
        // Send OPEN RPC. This can fail if the server on other end is is not up.
        // Pass the version that was obtained while setting the node to OFFLINE.
        RegionOpeningState regionOpenState = serverManager.sendRegionOpen(plan
            .getDestination(), state.getRegion(), versionOfOfflineNode);
        if (regionOpenState == RegionOpeningState.ALREADY_OPENED) {
          processAlreadyOpenedRegion(state.getRegion(), plan.getDestination());
        } else if (regionOpenState == RegionOpeningState.FAILED_OPENING) {
          // Failed opening this region
          throw new Exception("Get regionOpeningState=" + regionOpenState);
        }
        break;
      } catch (Throwable t) {
        if (t instanceof RemoteException) {
          t = ((RemoteException) t).unwrapRemoteException();
          if (t instanceof RegionAlreadyInTransitionException) {
            regionAlreadyInTransitionException = true;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed assignment in: " + plan.getDestination() + " due to "
                  + t.getMessage());
            }
          }
        }
        LOG.warn("Failed assignment of "
            + state.getRegion().getRegionNameAsString()
            + " to "
            + plan.getDestination()
            + ", trying to assign "
            + (regionAlreadyInTransitionException ? "to the same region server"
                + " because of RegionAlreadyInTransitionException;" : "elsewhere instead; ")
            + "retry=" + i, t);
        // Clean out plan we failed execute and one that doesn't look like it'll
        // succeed anyways; we need a new plan!
        // Transition back to OFFLINE
        regionStates.updateRegionState(
          state.getRegion(), RegionState.State.OFFLINE);
        // If region opened on destination of present plan, reassigning to new
        // RS may cause double assignments. In case of RegionAlreadyInTransitionException
        // reassigning to same RS.
        RegionPlan newPlan = plan;
        if (!regionAlreadyInTransitionException) {
          // Force a new plan and reassign. Will return null if no servers.
          newPlan = getRegionPlan(state, plan.getDestination(), true);
        }
        if (newPlan == null) {
          this.timeoutMonitor.setAllRegionServersOffline(true);
          LOG.warn("Unable to find a viable location to assign region " +
            state.getRegion().getRegionNameAsString());
          return;
        }
      }
    }
  }

  private void processAlreadyOpenedRegion(HRegionInfo region, ServerName sn) {
    // Remove region from in-memory transition and unassigned node from ZK
    // While trying to enable the table the regions of the table were
    // already enabled.
    LOG.debug("ALREADY_OPENED region " + region.getRegionNameAsString()
        + " to " + sn);
    String encodedRegionName = region.getEncodedName();
    try {
      ZKAssign.deleteOfflineNode(master.getZooKeeper(), encodedRegionName);
    } catch (KeeperException.NoNodeException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("The unassigned node " + encodedRegionName
            + " doesnot exist.");
      }
    } catch (KeeperException e) {
      master.abort(
          "Error deleting OFFLINED node in ZK for transition ZK node ("
              + encodedRegionName + ")", e);
    }

    regionStates.regionOnline(region, sn);
  }

  private boolean isDisabledorDisablingRegionInRIT(final HRegionInfo region) {
    String tableName = region.getTableNameAsString();
    boolean disabled = this.zkTable.isDisabledTable(tableName);
    if (disabled || this.zkTable.isDisablingTable(tableName)) {
      LOG.info("Table " + tableName + (disabled ? " disabled;" : " disabling;") +
        " skipping assign of " + region.getRegionNameAsString());
      offlineDisabledRegion(region);
      return true;
    }
    return false;
  }

  /**
   * Set region as OFFLINED up in zookeeper
   *
   * @param state
   * @param hijack
   *          - true if needs to be hijacked and reassigned, false otherwise.
   * @param regionAlreadyInTransitionException  
   *          - true if we need to retry assignment because of RegionAlreadyInTransitionException.       
   * @return the version of the offline node if setting of the OFFLINE node was
   *         successful, -1 otherwise.
   */
  int setOfflineInZooKeeper(final RegionState state, boolean hijack,
      boolean regionAlreadyInTransitionException) {
    // In case of reassignment the current state in memory need not be
    // OFFLINE. 
    if (!hijack && !state.isClosed() && !state.isOffline()) {
      if (!regionAlreadyInTransitionException ) {
        String msg = "Unexpected state : " + state + " .. Cannot transit it to OFFLINE.";
        this.master.abort(msg, new IllegalStateException(msg));
        return -1;
      } else {
        LOG.debug("Unexpected state : " + state
            + " but retrying to assign because RegionAlreadyInTransitionException.");
      }
    }
    boolean allowZNodeCreation = false;
    // Under reassignment if the current state is PENDING_OPEN
    // or OPENING then refresh the in-memory state to PENDING_OPEN. This is
    // important because if the region was in
    // RS_OPENING state for a long time the master will try to force the znode
    // to OFFLINE state meanwhile the RS could have opened the corresponding
    // region and the state in znode will be RS_ZK_REGION_OPENED.
    // For all other cases we can change the in-memory state to OFFLINE.
    if (hijack &&
        (state.getState().equals(RegionState.State.PENDING_OPEN) ||
            state.getState().equals(RegionState.State.OPENING))) {
      regionStates.updateRegionState(state.getRegion(),
        RegionState.State.PENDING_OPEN);
      allowZNodeCreation = false;
    } else {
      regionStates.updateRegionState(state.getRegion(),
        RegionState.State.OFFLINE);
      allowZNodeCreation = true;
    }
    int versionOfOfflineNode = -1;
    try {
      // get the version after setting the znode to OFFLINE
      versionOfOfflineNode = ZKAssign.createOrForceNodeOffline(master.getZooKeeper(),
          state.getRegion(), this.master.getServerName(),
          hijack, allowZNodeCreation);
      if (versionOfOfflineNode == -1) {
        LOG.warn("Attempted to create/force node into OFFLINE state before "
            + "completing assignment but failed to do so for " + state);
        return -1;
      }
    } catch (KeeperException e) {
      master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      return -1;
    }
    return versionOfOfflineNode;
  }

  /**
   * Set region as OFFLINED up in zookeeper asynchronously.
   * @param state
   * @return True if we succeeded, false otherwise (State was incorrect or failed
   * updating zk).
   */
  boolean asyncSetOfflineInZooKeeper(final RegionState state,
      final AsyncCallback.StringCallback cb, final Object ctx) {
    if (!state.isClosed() && !state.isOffline()) {
      this.master.abort("Unexpected state trying to OFFLINE; " + state,
        new IllegalStateException());
      return false;
    }
    regionStates.updateRegionState(
      state.getRegion(), RegionState.State.OFFLINE);
    try {
      ZKAssign.asyncCreateNodeOffline(master.getZooKeeper(), state.getRegion(),
        this.master.getServerName(), cb, ctx);
    } catch (KeeperException e) {
      if (e instanceof NodeExistsException) {
        LOG.warn("Node for " + state.getRegion() + " already exists");
      } else {
        master.abort("Unexpected ZK exception creating/setting node OFFLINE", e);
      }
      return false;
    }
    return true;
  }

  /**
   * @param state
   * @return Plan for passed <code>state</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  RegionPlan getRegionPlan(final RegionState state,
      final boolean forceNewPlan) {
    return getRegionPlan(state, null, forceNewPlan);
  }

  /**
   * @param state
   * @param serverToExclude Server to exclude (we know its bad). Pass null if
   * all servers are thought to be assignable.
   * @param forceNewPlan If true, then if an existing plan exists, a new plan
   * will be generated.
   * @return Plan for passed <code>state</code> (If none currently, it creates one or
   * if no servers to assign, it returns null).
   */
  RegionPlan getRegionPlan(final RegionState state,
      final ServerName serverToExclude, final boolean forceNewPlan) {
    // Pickup existing plan or make a new one
    final String encodedName = state.getRegion().getEncodedName();
    final List<ServerName> destServers =
      serverManager.createDestinationServersList(serverToExclude);

    if (destServers.isEmpty()){
      LOG.warn("Can't move the region " + encodedName +
        ", there is no destination server available.");
      return null;
    }

    RegionPlan randomPlan = null;
    boolean newPlan = false;
    RegionPlan existingPlan = null;

    synchronized (this.regionPlans) {
      existingPlan = this.regionPlans.get(encodedName);

      if (existingPlan != null && existingPlan.getDestination() != null) {
        LOG.debug("Found an existing plan for " +
            state.getRegion().getRegionNameAsString() +
       " destination server is " + existingPlan.getDestination().toString());
      }

      if (forceNewPlan
          || existingPlan == null
          || existingPlan.getDestination() == null
          || !destServers.contains(existingPlan.getDestination())) {
        newPlan = true;
        randomPlan = new RegionPlan(state.getRegion(), null,
            balancer.randomAssignment(state.getRegion(), destServers));
        this.regionPlans.put(encodedName, randomPlan);
      }
    }

    if (newPlan) {
      LOG.debug("No previous transition plan was found (or we are ignoring " +
        "an existing plan) for " + state.getRegion().getRegionNameAsString() +
        " so generated a random one; " + randomPlan + "; " +
        serverManager.countOfRegionServers() +
               " (online=" + serverManager.getOnlineServers().size() +
               ", available=" + destServers.size() + ") available servers");
        return randomPlan;
      }
    LOG.debug("Using pre-existing plan for region " +
               state.getRegion().getRegionNameAsString() + "; plan=" + existingPlan);
      return existingPlan;
  }

  /**
   * Loop through the deadNotExpired server list and remove them from the
   * servers.
   * @param servers
   * @deprecated the method is now available in ServerManager - deprecated in 0.96
   */
  @Deprecated
  void removeDeadNotExpiredServers(List<ServerName> servers) {
    this.serverManager.removeDeadNotExpiredServers(servers);
  }

  /**
   * Unassign the list of regions. Configuration knobs:
   * hbase.bulk.waitbetween.reopen indicates the number of milliseconds to
   * wait before unassigning another region from this region server
   *
   * @param regions
   * @throws InterruptedException
   */
  public void unassign(List<HRegionInfo> regions) {
    int waitTime = this.master.getConfiguration().getInt(
        "hbase.bulk.waitbetween.reopen", 0);
    for (HRegionInfo region : regions) {
      if (regionStates.isRegionInTransition(region))
        continue;
      unassign(region, false);
      while (regionStates.isRegionInTransition(region)) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
      }
      if (waitTime > 0)
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          // Do nothing, continue
        }
    }
  }

  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   */
  public void unassign(HRegionInfo region) {
    unassign(region, false);
  }


  /**
   * Unassigns the specified region.
   * <p>
   * Updates the RegionState and sends the CLOSE RPC unless region is being
   * split by regionserver; then the unassign fails (silently) because we
   * presume the region being unassigned no longer exists (its been split out
   * of existence). TODO: What to do if split fails and is rolled back and
   * parent is revivified?
   * <p>
   * If a RegionPlan is already set, it will remain.
   *
   * @param region server to be unassigned
   * @param force if region should be closed even if already closing
   */
  public void unassign(HRegionInfo region, boolean force, ServerName dest) {
    // TODO: Method needs refactoring.  Ugly buried returns throughout.  Beware!
    LOG.debug("Starting unassignment of region " +
      region.getRegionNameAsString() + " (offlining)");

    // Check if this region is currently assigned
    if (!regionStates.isRegionAssigned(region)) {
      LOG.debug("Attempted to unassign region " +
        region.getRegionNameAsString() + " but it is not " +
        "currently assigned anywhere");
      return;
    }
    String encodedName = region.getEncodedName();
    // Grab the state of this region and synchronize on it
    int versionOfClosingNode = -1;
    // We need a lock here as we're going to do a put later and we don't want multiple states
    //  creation
    ReentrantLock lock = locker.acquireLock(encodedName);
    RegionState state = regionStates.getRegionTransitionState(encodedName);
    try {
      if (state == null) {
        // Create the znode in CLOSING state
        try {
          versionOfClosingNode = ZKAssign.createNodeClosing(
            master.getZooKeeper(), region, master.getServerName());
          if (versionOfClosingNode == -1) {
            LOG.debug("Attempting to unassign region " +
                region.getRegionNameAsString() + " but ZK closing node "
                + "can't be created.");
            return;
          }
        } catch (KeeperException ee) {
          Exception e = ee;
          if (e instanceof NodeExistsException) {
            // Handle race between master initiated close and regionserver
            // orchestrated splitting. See if existing node is in a
            // SPLITTING or SPLIT state.  If so, the regionserver started
            // an op on node before we could get our CLOSING in.  Deal.
            NodeExistsException nee = (NodeExistsException)e;
            String path = nee.getPath();
            try {
              if (isSplitOrSplitting(path)) {
                LOG.debug(path + " is SPLIT or SPLITTING; " +
                  "skipping unassign because region no longer exists -- its split");
                return;
              }
            } catch (KeeperException.NoNodeException ke) {
              LOG.warn("Failed getData on SPLITTING/SPLIT at " + path +
                "; presuming split and that the region to unassign, " +
                encodedName + ", no longer exists -- confirm", ke);
              return;
            } catch (KeeperException ke) {
              LOG.error("Unexpected zk state", ke);
            } catch (DeserializationException de) {
              LOG.error("Failed parse", de);
            }
          }
          // If we get here, don't understand whats going on -- abort.
          master.abort("Unexpected ZK exception creating node CLOSING", e);
          return;
        }
        state = regionStates.updateRegionState(region, RegionState.State.PENDING_CLOSE);
      } else if (force && (state.isPendingClose() || state.isClosing())) {
        LOG.debug("Attempting to unassign region " + region.getRegionNameAsString() +
          " which is already " + state.getState()  +
          " but forcing to send a CLOSE RPC again ");
        state.updateTimestampToNow();
      } else {
        LOG.debug("Attempting to unassign region " +
          region.getRegionNameAsString() + " but it is " +
          "already in transition (" + state.getState() + ", force=" + force + ")");
        return;
      }
    } finally {
      lock.unlock();
    }

    // Send CLOSE RPC
    ServerName server = state.getServerName();
    // ClosedRegionhandler can remove the server from this.regions
    if (server == null) {
      // delete the node. if no node exists need not bother.
      deleteClosingOrClosedNode(region);
      return;
    }

    try {
      // TODO: We should consider making this look more like it does for the
      // region open where we catch all throwables and never abort
      if (serverManager.sendRegionClose(server, state.getRegion(),
        versionOfClosingNode, dest)) {
        LOG.debug("Sent CLOSE to " + server + " for region " +
          region.getRegionNameAsString());
        return;
      }
      // This never happens. Currently regionserver close always return true.
      LOG.warn("Server " + server + " region CLOSE RPC returned false for " +
        region.getRegionNameAsString());
    } catch (Throwable t) {
      if (t instanceof RemoteException) {
        t = ((RemoteException)t).unwrapRemoteException();
      }
      if (t instanceof NotServingRegionException) {
        // Presume that master has stale data.  Presume remote side just split.
        // Presume that the split message when it comes in will fix up the master's
        // in memory cluster state.
        if (checkIfRegionBelongsToDisabling(region)) {
          // Remove from the regionsinTransition map
          LOG.info("While trying to recover the table "
              + region.getTableNameAsString()
              + " to DISABLED state the region " + region
              + " was offlined but the table was in DISABLING state");
          regionStates.regionOffline(region);
          deleteClosingOrClosedNode(region);
        }
      } else if (t instanceof RegionAlreadyInTransitionException) {
        // RS is already processing this region, only need to update the timestamp
        LOG.debug("update " + state + " the timestamp.");
        state.updateTimestampToNow();
      }
      LOG.info("Server " + server + " returned " + t + " for " +
        region.getRegionNameAsString(), t);
      // Presume retry or server will expire.
    }
  }

  public void unassign(HRegionInfo region, boolean force){
     unassign(region, force, null);
  }

  /**
   *
   * @param region regioninfo of znode to be deleted.
   */
  public void deleteClosingOrClosedNode(HRegionInfo region) {
    try {
      if (!ZKAssign.deleteNode(master.getZooKeeper(), region.getEncodedName(),
          EventHandler.EventType.M_ZK_REGION_CLOSING)) {
        boolean deleteNode = ZKAssign.deleteNode(master.getZooKeeper(), region
            .getEncodedName(), EventHandler.EventType.RS_ZK_REGION_CLOSED);
        // TODO : We don't abort if the delete node returns false. Is there any
        // such corner case?
        if (!deleteNode) {
          LOG.error("The deletion of the CLOSED node for the region "
              + region.getEncodedName() + " returned " + deleteNode);
        }
      }
    } catch (NoNodeException e) {
      LOG.debug("CLOSING/CLOSED node for the region " + region.getEncodedName()
          + " already deleted");
    } catch (KeeperException ke) {
      master.abort(
          "Unexpected ZK exception deleting node CLOSING/CLOSED for the region "
              + region.getEncodedName(), ke);
      return;
    }
  }

  /**
   * @param path
   * @return True if znode is in SPLIT or SPLITTING state.
   * @throws KeeperException Can happen if the znode went away in meantime.
   * @throws DeserializationException
   */
  private boolean isSplitOrSplitting(final String path)
  throws KeeperException, DeserializationException {
    boolean result = false;
    // This may fail if the SPLIT or SPLITTING znode gets cleaned up before we
    // can get data from it.
    byte [] data = ZKAssign.getData(master.getZooKeeper(), path);
    if (data == null) return false;
    RegionTransition rt = RegionTransition.parseFrom(data);
    switch (rt.getEventType()) {
    case RS_ZK_REGION_SPLIT:
    case RS_ZK_REGION_SPLITTING:
      result = true;
      break;
    default:
      break;
    }
    return result;
  }

  /**
   * Waits until the specified region has completed assignment.
   * <p>
   * If the region is already assigned, returns immediately.  Otherwise, method
   * blocks until the region is assigned.
   * @param regionInfo region to wait on assignment for
   * @throws InterruptedException
   */
  public void waitForAssignment(HRegionInfo regionInfo)
  throws InterruptedException {
    while(!this.master.isStopped() &&
        !regionStates.isRegionAssigned(regionInfo)) {
      // We should receive a notification, but it's
      //  better to have a timeout to recheck the condition here:
      //  it lowers the impact of a race condition if any
      regionStates.waitForUpdate(100);
    }
  }

  /**
   * Assigns the ROOT region.
   * <p>
   * Assumes that ROOT is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly unsets the current root region location in ZooKeeper and assigns
   * ROOT to a random RegionServer.
   * @throws KeeperException
   */
  public void assignRoot() throws KeeperException {
    RootRegionTracker.deleteRootLocation(this.master.getZooKeeper());
    assign(HRegionInfo.ROOT_REGIONINFO, true);
  }

  /**
   * Assigns the META region.
   * <p>
   * Assumes that META is currently closed and is not being actively served by
   * any RegionServer.
   * <p>
   * Forcibly assigns META to a random RegionServer.
   */
  public void assignMeta() {
    // Force assignment to a random server
    assign(HRegionInfo.FIRST_META_REGIONINFO, true);
  }

  /**
   * Assigns all user regions to online servers. Use round-robin assignment.
   *
   * @param regions
   * @throws IOException
   * @throws InterruptedException
   */
  public void assignUserRegionsToOnlineServers(List<HRegionInfo> regions)
      throws IOException,
      InterruptedException {
    List<ServerName> destServers = serverManager.createDestinationServersList();
    assignUserRegions(regions, destServers);
  }

  /**
   * Assigns all user regions, if any.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown
   * @throws InterruptedException
   * @throws IOException
   */
  public void assignUserRegions(List<HRegionInfo> regions, List<ServerName> servers)
  throws IOException, InterruptedException {
    if (regions == null)
      return;
    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    // Generate a round-robin bulk assignment plan
    bulkPlan = balancer.roundRobinAssignment(regions, servers);
    LOG.info("Bulk assigning " + regions.size() + " region(s) round-robin across " +
               servers.size() + " server(s)");
    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(this.master, bulkPlan, this);
    ba.bulkAssign();
    LOG.info("Bulk assigning done");
  }

  // TODO: This method seems way wrong.  Why would we mark a table enabled based
  // off a single region?  We seem to call this on bulk assign on startup which
  // isn't too bad but then its also called in assign.  It makes the enabled
  // flag up in zk meaningless.  St.Ack
  private void setEnabledTable(HRegionInfo hri) {
    String tableName = hri.getTableNameAsString();
    boolean isTableEnabled = this.zkTable.isEnabledTable(tableName);
    if (!isTableEnabled) {
      setEnabledTable(tableName);
    }
  }

  /**
   * Assigns all user regions, if any exist.  Used during cluster startup.
   * <p>
   * This is a synchronous call and will return once every region has been
   * assigned.  If anything fails, an exception is thrown and the cluster
   * should be shutdown.
   * @throws InterruptedException
   * @throws IOException
   */
  public void assignAllUserRegions() throws IOException, InterruptedException {
    // Skip assignment for regions of tables in DISABLING state because during clean cluster startup
    // no RS is alive and regions map also doesn't have any information about the regions.
    // See HBASE-6281.
    Set<String> disablingAndDisabledTables = new HashSet<String>(this.disablingTables);
    disablingAndDisabledTables.addAll(this.zkTable.getDisabledTables());
    // Scan META for all user regions, skipping any disabled tables
    Map<HRegionInfo, ServerName> allRegions = MetaReader.fullScan(catalogTracker,
        disablingAndDisabledTables, true);
    if (allRegions == null || allRegions.isEmpty()) return;

    // Get all available servers
    List<ServerName> destServers = serverManager.createDestinationServersList();

    // If there are no servers we need not proceed with region assignment.
    if (destServers.isEmpty()) return;

    // Determine what type of assignment to do on startup
    boolean retainAssignment = master.getConfiguration().
      getBoolean("hbase.master.startup.retainassign", true);

    Map<ServerName, List<HRegionInfo>> bulkPlan = null;
    if (retainAssignment) {
      // Reuse existing assignment info
      bulkPlan = balancer.retainAssignment(allRegions, destServers);
    } else {
      // assign regions in round-robin fashion
      assignUserRegions(new ArrayList<HRegionInfo>(allRegions.keySet()), destServers);
      for (HRegionInfo hri : allRegions.keySet()) {
        setEnabledTable(hri);
      }
      return;
    }
    LOG.info("Bulk assigning " + allRegions.size() + " region(s) across " +
      destServers.size() + " server(s), retainAssignment=" + retainAssignment);

    // Use fixed count thread pool assigning.
    BulkAssigner ba = new StartupBulkAssigner(this.master, bulkPlan, this);
    ba.bulkAssign();
    for (HRegionInfo hri : allRegions.keySet()) {
      setEnabledTable(hri);
    }
    LOG.info("Bulk assigning done");
  }

  /**
   * Run bulk assign on startup.  Does one RCP per regionserver passing a
   * batch of reginons using {@link SingleServerBulkAssigner}.
   * Uses default {@link #getUncaughtExceptionHandler()}
   * which will abort the Server if exception.
   */
  static class StartupBulkAssigner extends BulkAssigner {
    final Map<ServerName, List<HRegionInfo>> bulkPlan;
    final AssignmentManager assignmentManager;

    StartupBulkAssigner(final Server server,
        final Map<ServerName, List<HRegionInfo>> bulkPlan,
        final AssignmentManager am) {
      super(server);
      this.bulkPlan = bulkPlan;
      this.assignmentManager = am;
    }

    @Override
    public boolean bulkAssign(boolean sync) throws InterruptedException,
        IOException {
      // Disable timing out regions in transition up in zk while bulk assigning.
      this.assignmentManager.timeoutMonitor.bulkAssign(true);
      try {
        return super.bulkAssign(sync);
      } finally {
        // Reenable timing out regions in transition up in zi.
        this.assignmentManager.timeoutMonitor.bulkAssign(false);
      }
    }

    @Override
    protected String getThreadNamePrefix() {
      return this.server.getServerName() + "-StartupBulkAssigner";
    }

    @Override
    protected void populatePool(java.util.concurrent.ExecutorService pool) {
      for (Map.Entry<ServerName, List<HRegionInfo>> e: this.bulkPlan.entrySet()) {
        pool.execute(new SingleServerBulkAssigner(e.getKey(), e.getValue(),
          this.assignmentManager));
      }
    }

    /**
     *
     * @param timeout How long to wait.
     * @return true if done.
     */
    @Override
    protected boolean waitUntilDone(final long timeout)
    throws InterruptedException {
      Set<HRegionInfo> regionSet = new HashSet<HRegionInfo>();
      for (List<HRegionInfo> regionList : bulkPlan.values()) {
        regionSet.addAll(regionList);
      }
      return this.assignmentManager.waitUntilNoRegionsInTransition(timeout, regionSet);
    }

    @Override
    protected long getTimeoutOnRIT() {
      // Guess timeout.  Multiply the number of regions on a random server
      // by how long we thing one region takes opening.
      long perRegionOpenTimeGuesstimate =
        this.server.getConfiguration().getLong("hbase.bulk.assignment.perregion.open.time", 1000);
      int regionsPerServer =
        this.bulkPlan.entrySet().iterator().next().getValue().size();
      long timeout = perRegionOpenTimeGuesstimate * regionsPerServer;
      LOG.debug("Timeout-on-RIT=" + timeout);
      return timeout;
    }
  }

  /**
   * Bulk user region assigner.
   * If failed assign, lets timeout in RIT do cleanup.
   */
  static class GeneralBulkAssigner extends StartupBulkAssigner {
    GeneralBulkAssigner(final Server server,
        final Map<ServerName, List<HRegionInfo>> bulkPlan,
        final AssignmentManager am) {
      super(server, bulkPlan, am);
    }

    @Override
    protected UncaughtExceptionHandler getUncaughtExceptionHandler() {
      return new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          LOG.warn("Assigning regions in " + t.getName(), e);
        }
      };
    }
  }

  /**
   * Manage bulk assigning to a server.
   */
  static class SingleServerBulkAssigner implements Runnable {
    private final ServerName regionserver;
    private final List<HRegionInfo> regions;
    private final AssignmentManager assignmentManager;

    SingleServerBulkAssigner(final ServerName regionserver,
        final List<HRegionInfo> regions, final AssignmentManager am) {
      this.regionserver = regionserver;
      this.regions = regions;
      this.assignmentManager = am;
    }
    @Override
    public void run() {
      this.assignmentManager.assign(this.regionserver, this.regions);
    }
  }

  /**
   * Wait until no regions in transition.
   * @param timeout How long to wait.
   * @return True if nothing in regions in transition.
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout)
  throws InterruptedException {
    // Blocks until there are no regions in transition. It is possible that
    // there
    // are regions in transition immediately after this returns but guarantees
    // that if it returns without an exception that there was a period of time
    // with no regions in transition from the point-of-view of the in-memory
    // state of the Master.
    final long endTime = System.currentTimeMillis() + timeout;

    while (!this.master.isStopped() && regionStates.isRegionsInTransition()
        && endTime > System.currentTimeMillis()) {
      regionStates.waitForUpdate(100);
    }

    return !regionStates.isRegionsInTransition();
  }

  /**
   * Wait until no regions from set regions are in transition.
   * @param timeout How long to wait.
   * @param regions set of regions to wait for. It will be modified by this method.
   * @return True if none of the regions in the set is in transition
   * @throws InterruptedException
   */
  boolean waitUntilNoRegionsInTransition(final long timeout, Set<HRegionInfo> regions)
    throws InterruptedException {
    final long endTime = System.currentTimeMillis() + timeout;

    // We're not synchronizing on regionsInTransition now because we don't use any iterator.
    while (!regions.isEmpty() && !this.master.isStopped() && endTime > System.currentTimeMillis()) {
      Iterator<HRegionInfo> regionInfoIterator = regions.iterator();
      while (regionInfoIterator.hasNext()) {
        HRegionInfo hri = regionInfoIterator.next();
        if (!regionStates.isRegionInTransition(hri)) {
          regionInfoIterator.remove();
        }
      }

      if (!regions.isEmpty()) {
        regionStates.waitForUpdate(100);
      }
    }

    return regions.isEmpty();
  }

  /**
   * Rebuild the list of user regions and assignment information.
   * <p>
   * Returns a map of servers that are not found to be online and the regions
   * they were hosting.
   * @return map of servers not online to their assigned regions, as stored
   *         in META
   * @throws IOException
   */
  Map<ServerName, List<Pair<HRegionInfo, Result>>> rebuildUserRegions()
  throws IOException, KeeperException {
    // Region assignment from META
    List<Result> results = MetaReader.fullScan(this.catalogTracker);
    // Get any new but slow to checkin region server that joined the cluster
    Set<ServerName> onlineServers = serverManager.getOnlineServers().keySet();
    // Map of offline servers and their regions to be returned
    Map<ServerName, List<Pair<HRegionInfo,Result>>> offlineServers =
      new TreeMap<ServerName, List<Pair<HRegionInfo, Result>>>();
    // Iterate regions in META
    for (Result result : results) {
      boolean disabled = false;
      boolean disablingOrEnabling = false;
      Pair<HRegionInfo, ServerName> region = HRegionInfo.getHRegionInfoAndServerName(result);
      if (region == null) continue;
      HRegionInfo regionInfo = region.getFirst();
      ServerName regionLocation = region.getSecond();
      if (regionInfo == null) continue;
      regionStates.createRegionState(regionInfo);
      String tableName = regionInfo.getTableNameAsString();
      if (regionLocation == null) {
        // regionLocation could be null if createTable didn't finish properly.
        // When createTable is in progress, HMaster restarts.
        // Some regions have been added to .META., but have not been assigned.
        // When this happens, the region's table must be in ENABLING state.
        // It can't be in ENABLED state as that is set when all regions are
        // assigned.
        // It can't be in DISABLING state, because DISABLING state transitions
        // from ENABLED state when application calls disableTable.
        // It can't be in DISABLED state, because DISABLED states transitions
        // from DISABLING state.
        if (false == checkIfRegionsBelongsToEnabling(regionInfo)) {
          LOG.warn("Region " + regionInfo.getEncodedName() +
            " has null regionLocation." + " But its table " + tableName +
            " isn't in ENABLING state.");
        }
        addTheTablesInPartialState(this.disablingTables, this.enablingTables, regionInfo,
            tableName);
      } else if (!onlineServers.contains(regionLocation)) {
        // Region is located on a server that isn't online
        List<Pair<HRegionInfo, Result>> offlineRegions =
          offlineServers.get(regionLocation);
        if (offlineRegions == null) {
          offlineRegions = new ArrayList<Pair<HRegionInfo,Result>>(1);
          offlineServers.put(regionLocation, offlineRegions);
        }
        offlineRegions.add(new Pair<HRegionInfo,Result>(regionInfo, result));
        disabled = checkIfRegionBelongsToDisabled(regionInfo);
        disablingOrEnabling = addTheTablesInPartialState(this.disablingTables,
            this.enablingTables, regionInfo, tableName);
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        enableTableIfNotDisabledOrDisablingOrEnabling(disabled,
            disablingOrEnabling, tableName);
      } else {
        // If region is in offline and split state check the ZKNode
        if (regionInfo.isOffline() && regionInfo.isSplit()) {
          String node = ZKAssign.getNodeName(this.watcher, regionInfo
              .getEncodedName());
          Stat stat = new Stat();
          byte[] data = ZKUtil.getDataNoWatch(this.watcher, node, stat);
          // If znode does not exist dont consider this region
          if (data == null) {
            LOG.debug("Region "	+  regionInfo.getRegionNameAsString()
               + " split is completed. Hence need not add to regions list");
            continue;
          }
        }
        // Region is being served and on an active server
        // add only if region not in disabled and enabling table
        if (!checkIfRegionBelongsToDisabled(regionInfo)
            && !checkIfRegionsBelongsToEnabling(regionInfo)) {
          regionStates.regionOnline(regionInfo, regionLocation);
        }
        disablingOrEnabling = addTheTablesInPartialState(this.disablingTables,
            this.enablingTables, regionInfo, tableName);
        disabled = checkIfRegionBelongsToDisabled(regionInfo);
        // need to enable the table if not disabled or disabling or enabling
        // this will be used in rolling restarts
        enableTableIfNotDisabledOrDisablingOrEnabling(disabled,
            disablingOrEnabling, tableName);
      }
    }
    return offlineServers;
  }

  private void enableTableIfNotDisabledOrDisablingOrEnabling(boolean disabled,
      boolean disablingOrEnabling, String tableName) {
    if (!disabled && !disablingOrEnabling
        && !getZKTable().isEnabledTable(tableName)) {
      setEnabledTable(tableName);
    }
  }

  private Boolean addTheTablesInPartialState(Set<String> disablingTables,
      Set<String> enablingTables, HRegionInfo regionInfo,
      String disablingTableName) {
    if (checkIfRegionBelongsToDisabling(regionInfo)) {
      disablingTables.add(disablingTableName);
      return true;
    } else if (checkIfRegionsBelongsToEnabling(regionInfo)) {
      enablingTables.add(disablingTableName);
      return true;
    }
    return false;
  }

  /**
   * Recover the tables that were not fully moved to DISABLED state. These
   * tables are in DISABLING state when the master restarted/switched.
   *
   * @param disablingTables
   * @return
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private boolean recoverTableInDisablingState(Set<String> disablingTables)
      throws KeeperException, TableNotFoundException, IOException {
    boolean isWatcherCreated = false;
    if (disablingTables.size() != 0) {
      // Create a watcher on the zookeeper node
      ZKUtil.listChildrenAndWatchForNewChildren(watcher,
          watcher.assignmentZNode);
      isWatcherCreated = true;
      for (String tableName : disablingTables) {
        // Recover by calling DisableTableHandler
        LOG.info("The table " + tableName
            + " is in DISABLING state.  Hence recovering by moving the table"
            + " to DISABLED state.");
        new DisableTableHandler(this.master, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
    return isWatcherCreated;
  }

  /**
   * Recover the tables that are not fully moved to ENABLED state. These tables
   * are in ENABLING state when the master restarted/switched
   *
   * @param enablingTables
   * @param isWatcherCreated
   * @throws KeeperException
   * @throws TableNotFoundException
   * @throws IOException
   */
  private void recoverTableInEnablingState(Set<String> enablingTables,
      boolean isWatcherCreated) throws KeeperException, TableNotFoundException,
      IOException {
    if (enablingTables.size() != 0) {
      if (false == isWatcherCreated) {
        ZKUtil.listChildrenAndWatchForNewChildren(watcher,
            watcher.assignmentZNode);
      }
      for (String tableName : enablingTables) {
        // Recover by calling EnableTableHandler
        LOG.info("The table " + tableName
            + " is in ENABLING state.  Hence recovering by moving the table"
            + " to ENABLED state.");
        // enableTable in sync way during master startup,
        // no need to invoke coprocessor
        new EnableTableHandler(this.master, tableName.getBytes(),
            catalogTracker, this, true).process();
      }
    }
  }

  private boolean checkIfRegionsBelongsToEnabling(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isEnablingTable(tableName);
  }

  private boolean checkIfRegionBelongsToDisabled(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isDisabledTable(tableName);
  }

  private boolean checkIfRegionBelongsToDisabling(HRegionInfo regionInfo) {
    String tableName = regionInfo.getTableNameAsString();
    return getZKTable().isDisablingTable(tableName);
  }

  /**
   * Processes list of dead servers from result of META scan and regions in RIT
   * <p>
   * This is used for failover to recover the lost regions that belonged to
   * RegionServers which failed while there was no active master or regions
   * that were in RIT.
   * <p>
   *
   * @param deadServers
   *          The list of dead servers which failed while there was no active
   *          master. Can be null.
   * @param nodes
   *          The regions in RIT
   * @throws IOException
   * @throws KeeperException
   */
  private void processDeadServersAndRecoverLostRegions(
      Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers, List<String> nodes)
  throws IOException, KeeperException {
    processDeadServers(deadServers, nodes);
    if (!nodes.isEmpty()) {
      for (String encodedRegionName : nodes) {
        processRegionInTransition(encodedRegionName, null, deadServers);
      }
    }
  }

  private void processDeadServers(Map<ServerName, List<Pair<HRegionInfo, Result>>> deadServers,
      final List<String> nodes)
  throws KeeperException, IOException {
    if (deadServers == null) return;
    Set<ServerName> actualDeadServers = this.serverManager.getDeadServers();
    for (Map.Entry<ServerName, List<Pair<HRegionInfo, Result>>> deadServer: deadServers.entrySet()) {
      // skip regions of dead servers because SSH will process regions during rs expiration.
      // see HBASE-5916
      if (actualDeadServers.contains(deadServer.getKey())) {
        for (Pair<HRegionInfo, Result> deadRegion : deadServer.getValue()) {
          nodes.remove(deadRegion.getFirst().getEncodedName());
        }
        continue;
      }
      List<Pair<HRegionInfo, Result>> regions = deadServer.getValue();
      for (Pair<HRegionInfo, Result> region : regions) {
        HRegionInfo regionInfo = region.getFirst();
        Result result = region.getSecond();
        try {
          // If region was in transition (was in zk) force it offline for reassign.  Check if node
          // up in zk at all first.
          if (ZKUtil.checkExists(this.watcher,
              ZKAssign.getPath(this.watcher, regionInfo.getEncodedName())) != -1) {
            byte [] data = ZKAssign.getData(watcher, regionInfo.getEncodedName());
            if (data == null) {
              LOG.warn("No data in znode for " + regionInfo.getEncodedName());
              continue;
            }
            RegionTransition rt;
            try {
              rt = RegionTransition.parseFrom(data);
            } catch (DeserializationException e) {
              LOG.warn("Failed parse of znode data for " + regionInfo.getEncodedName(), e);
              continue;
            }

            // If zk node of this region has been updated by a live server,
            // we consider that this region is being handled.
            // So we should skip it and process it in processRegionsInTransition.
            ServerName sn = rt.getServerName();
            if (isServerOnline(sn)) {
              LOG.info("The region " + regionInfo.getEncodedName() + "is being handled on " + sn);
              continue;
            }
          }
          // Process with existing RS shutdown code
          boolean assign = ServerShutdownHandler.processDeadRegion(
              regionInfo, result, this, this.catalogTracker);
          if (assign) {
            ZKAssign.createOrForceNodeOffline(watcher, regionInfo,
                master.getServerName());
            if (!nodes.contains(regionInfo.getEncodedName())) {
              nodes.add(regionInfo.getEncodedName());
            }
          }
        } catch (KeeperException.NoNodeException nne) {
          // This is fine
        }
      }
    }
  }

  /**
   * Set Regions in transitions metrics.
   * This takes an iterator on the RegionInTransition map (CLSM), and is not synchronized.
   * This iterator is not fail fast, wich may lead to stale read; but that's better than
   * creating a copy of the map for metrics computation, as this method will be invoked
   * on a frequent interval.
   */
  public void updateRegionsInTransitionMetrics() {
    long currentTime = System.currentTimeMillis();
    int totalRITs = 0;
    int totalRITsOverThreshold = 0;
    long oldestRITTime = 0;
    int ritThreshold = this.master.getConfiguration().
      getInt(HConstants.METRICS_RIT_STUCK_WARNING_THRESHOLD, 60000);
    for (RegionState state: regionStates.getRegionsInTransition().values()) {
      totalRITs++;
      long ritTime = currentTime - state.getStamp();
      if (ritTime > ritThreshold) { // more than the threshold
        totalRITsOverThreshold++;
      }
      if (oldestRITTime < ritTime) {
        oldestRITTime = ritTime;
      }
    }
    if (this.masterMetrics != null) {
      this.masterMetrics.updateRITOldestAge(oldestRITTime);
      this.masterMetrics.updateRITCount(totalRITs);
      this.masterMetrics.updateRITCountOverThreshold(totalRITsOverThreshold);
    }
  }

  /**
   * @param region Region whose plan we are to clear.
   */
  void clearRegionPlan(final HRegionInfo region) {
    synchronized (this.regionPlans) {
      this.regionPlans.remove(region.getEncodedName());
    }
  }

  /**
   * Wait on region to clear regions-in-transition.
   * @param hri Region to wait on.
   * @throws IOException
   */
  public void waitOnRegionToClearRegionsInTransition(final HRegionInfo hri)
      throws IOException, InterruptedException {
    if (!regionStates.isRegionInTransition(hri)) return;
    RegionState rs = null;
    // There is already a timeout monitor on regions in transition so I
    // should not have to have one here too?
    while(!this.master.isStopped() && regionStates.isRegionInTransition(hri)) {
      LOG.info("Waiting on " + rs + " to clear regions-in-transition");
      regionStates.waitForUpdate(1000);
    }
    if (this.master.isStopped()) {
      LOG.info("Giving up wait on regions in " +
        "transition because stoppable.isStopped is set");
    }
  }

  /**
   * Update timers for all regions in transition going against the server in the
   * serversInUpdatingTimer.
   */
  public class TimerUpdater extends Chore {

    public TimerUpdater(final int period, final Stoppable stopper) {
      super("AssignmentTimerUpdater", period, stopper);
    }

    @Override
    protected void chore() {
      ServerName serverToUpdateTimer = null;
      while (!serversInUpdatingTimer.isEmpty() && !stopper.isStopped()) {
        if (serverToUpdateTimer == null) {
          serverToUpdateTimer = serversInUpdatingTimer.first();
        } else {
          serverToUpdateTimer = serversInUpdatingTimer
              .higher(serverToUpdateTimer);
        }
        if (serverToUpdateTimer == null) {
          break;
        }
        updateTimers(serverToUpdateTimer);
        serversInUpdatingTimer.remove(serverToUpdateTimer);
      }
    }
  }

  /**
   * Monitor to check for time outs on region transition operations
   */
  public class TimeoutMonitor extends Chore {
    private final int timeout;
    private boolean bulkAssign = false;
    private boolean allRegionServersOffline = false;
    private ServerManager serverManager;

    /**
     * Creates a periodic monitor to check for time outs on region transition
     * operations.  This will deal with retries if for some reason something
     * doesn't happen within the specified timeout.
     * @param period
   * @param stopper When {@link Stoppable#isStopped()} is true, this thread will
   * cleanup and exit cleanly.
     * @param timeout
     */
    public TimeoutMonitor(final int period, final Stoppable stopper,
        ServerManager serverManager,
        final int timeout) {
      super("AssignmentTimeoutMonitor", period, stopper);
      this.timeout = timeout;
      this.serverManager = serverManager;
    }

    /**
     * @param bulkAssign If true, we'll suspend checking regions in transition
     * up in zookeeper.  If false, will reenable check.
     * @return Old setting for bulkAssign.
     */
    public boolean bulkAssign(final boolean bulkAssign) {
      boolean result = this.bulkAssign;
      this.bulkAssign = bulkAssign;
      return result;
    }

    private synchronized void setAllRegionServersOffline(
      boolean allRegionServersOffline) {
      this.allRegionServersOffline = allRegionServersOffline;
    }

    @Override
    protected void chore() {
      // If bulkAssign in progress, suspend checks
      if (this.bulkAssign) return;
      boolean noRSAvailable = this.serverManager.createDestinationServersList().isEmpty();

      // Iterate all regions in transition checking for time outs
      long now = System.currentTimeMillis();
      // no lock concurrent access ok: we will be working on a copy, and it's java-valid to do
      //  a copy while another thread is adding/removing items
      for (RegionState regionState : regionStates.getRegionsInTransition().values()) {
        if (regionState.getStamp() + timeout <= now) {
          // decide on action upon timeout
          actOnTimeOut(regionState);
        } else if (this.allRegionServersOffline && !noRSAvailable) {
          RegionPlan existingPlan = regionPlans.get(regionState.getRegion()
              .getEncodedName());
          if (existingPlan == null
              || !this.serverManager.isServerOnline(existingPlan
                  .getDestination())) {
            // if some RSs just came back online, we can start the assignment
            // right away
            actOnTimeOut(regionState);
          }
        }
      }
      setAllRegionServersOffline(noRSAvailable);
    }

    private void actOnTimeOut(RegionState regionState) {
      HRegionInfo regionInfo = regionState.getRegion();
      LOG.info("Regions in transition timed out:  " + regionState);
      // Expired! Do a retry.
      switch (regionState.getState()) {
      case CLOSED:
        LOG.info("Region " + regionInfo.getEncodedName()
            + " has been CLOSED for too long, waiting on queued "
            + "ClosedRegionHandler to run or server shutdown");
        // Update our timestamp.
        regionState.updateTimestampToNow();
        break;
      case OFFLINE:
        LOG.info("Region has been OFFLINE for too long, " + "reassigning "
            + regionInfo.getRegionNameAsString() + " to a random server");
        invokeAssign(regionInfo);
        break;
      case PENDING_OPEN:
        LOG.info("Region has been PENDING_OPEN for too "
            + "long, reassigning region=" + regionInfo.getRegionNameAsString());
        invokeAssign(regionInfo);
        break;
      case OPENING:
        processOpeningState(regionInfo);
        break;
      case OPEN:
        LOG.error("Region has been OPEN for too long, " +
            "we don't know where region was opened so can't do anything");
        // TODO: do we need synchronization here?
        // could not synchronized on regionState since it can be
        // an new instance
        String encodedName = regionState.getRegion().getEncodedName();
        Lock lock = assignLocker.acquireLock(encodedName);
        try {
          regionState.updateTimestampToNow();
        } finally {
          lock.unlock();
        }
        break;

      case PENDING_CLOSE:
        LOG.info("Region has been PENDING_CLOSE for too "
            + "long, running forced unassign again on region="
            + regionInfo.getRegionNameAsString());
        invokeUnassign(regionInfo);
        break;
      case CLOSING:
        LOG.info("Region has been CLOSING for too " +
          "long, this should eventually complete or the server will " +
          "expire, send RPC again");
        invokeUnassign(regionInfo);
        break;

      case SPLIT:
      case SPLITTING:
        break;

      default:
        throw new IllegalStateException("Received event is not valid.");
      }
    }
  }

  private void processOpeningState(HRegionInfo regionInfo) {
    LOG.info("Region has been OPENING for too " + "long, reassigning region="
        + regionInfo.getRegionNameAsString());
    // Should have a ZK node in OPENING state
    try {
      String node = ZKAssign.getNodeName(watcher, regionInfo.getEncodedName());
      Stat stat = new Stat();
      byte [] data = ZKAssign.getDataNoWatch(watcher, node, stat);
      if (data == null) {
        LOG.warn("Data is null, node " + node + " no longer exists");
        return;
      }
      RegionTransition rt = RegionTransition.parseFrom(data);
      EventType et = rt.getEventType();
      if (et == EventType.RS_ZK_REGION_OPENED) {
        LOG.debug("Region has transitioned to OPENED, allowing "
            + "watched event handlers to process");
        return;
      } else if (et != EventType.RS_ZK_REGION_OPENING && et != EventType.RS_ZK_REGION_FAILED_OPEN ) {
        LOG.warn("While timing out a region, found ZK node in unexpected state: " + et);
        return;
      }
      invokeAssign(regionInfo);
    } catch (KeeperException ke) {
      LOG.error("Unexpected ZK exception timing out CLOSING region", ke);
      return;
    } catch (DeserializationException e) {
      LOG.error("Unexpected exception parsing CLOSING region", e);
      return;
    }
    return;
  }

  private void invokeAssign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new AssignCallable(this, regionInfo));
  }

  private void invokeUnassign(HRegionInfo regionInfo) {
    threadPoolExecutorService.submit(new UnAssignCallable(this, regionInfo));
  }

  public boolean isCarryingRoot(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.ROOT_REGIONINFO);
  }

  public boolean isCarryingMeta(ServerName serverName) {
    return isCarryingRegion(serverName, HRegionInfo.FIRST_META_REGIONINFO);
  }

  /**
   * Check if the shutdown server carries the specific region.
   * We have a bunch of places that store region location
   * Those values aren't consistent. There is a delay of notification.
   * The location from zookeeper unassigned node has the most recent data;
   * but the node could be deleted after the region is opened by AM.
   * The AM's info could be old when OpenedRegionHandler
   * processing hasn't finished yet when server shutdown occurs.
   * @return whether the serverName currently hosts the region
   */
  public boolean isCarryingRegion(ServerName serverName, HRegionInfo hri) {
    RegionTransition rt = null;
    try {
      byte [] data = ZKAssign.getData(master.getZooKeeper(), hri.getEncodedName());
      // This call can legitimately come by null
      rt = data == null? null: RegionTransition.parseFrom(data);
    } catch (KeeperException e) {
      master.abort("Exception reading unassigned node for region=" + hri.getEncodedName(), e);
    } catch (DeserializationException e) {
      master.abort("Exception parsing unassigned node for region=" + hri.getEncodedName(), e);
    }

    ServerName addressFromZK = rt != null? rt.getServerName():  null;
    if (addressFromZK != null) {
      // if we get something from ZK, we will use the data
      boolean matchZK = (addressFromZK != null &&
        addressFromZK.equals(serverName));
      LOG.debug("based on ZK, current region=" + hri.getRegionNameAsString() +
          " is on server=" + addressFromZK +
          " server being checked=: " + serverName);
      return matchZK;
    }

    ServerName addressFromAM = regionStates.getRegionServerOfRegion(hri);
    boolean matchAM = (addressFromAM != null &&
      addressFromAM.equals(serverName));
    LOG.debug("based on AM, current region=" + hri.getRegionNameAsString() +
      " is on server=" + (addressFromAM != null ? addressFromAM : "null") +
      " server being checked: " + serverName);

    return matchAM;
  }

  /**
   * Process shutdown server removing any assignments.
   * @param sn Server that went down.
   * @return list of regions in transition on this server
   */
  public List<RegionState> processServerShutdown(final ServerName sn) {
    // Clean out any existing assignment plans for this server
    synchronized (this.regionPlans) {
      for (Iterator <Map.Entry<String, RegionPlan>> i =
          this.regionPlans.entrySet().iterator(); i.hasNext();) {
        Map.Entry<String, RegionPlan> e = i.next();
        ServerName otherSn = e.getValue().getDestination();
        // The name will be null if the region is planned for a random assign.
        if (otherSn != null && otherSn.equals(sn)) {
          // Use iterator's remove else we'll get CME
          i.remove();
        }
      }
    }
    return regionStates.serverOffline(sn);
  }

  /**
   * Update inmemory structures.
   * @param sn Server that reported the split
   * @param parent Parent region that was split
   * @param a Daughter region A
   * @param b Daughter region B
   */
  public void handleSplitReport(final ServerName sn, final HRegionInfo parent,
      final HRegionInfo a, final HRegionInfo b) {
    regionOffline(parent);
    regionOnline(a, sn);
    regionOnline(b, sn);

    // There's a possibility that the region was splitting while a user asked
    // the master to disable, we need to make sure we close those regions in
    // that case. This is not racing with the region server itself since RS
    // report is done after the split transaction completed.
    if (this.zkTable.isDisablingOrDisabledTable(
        parent.getTableNameAsString())) {
      unassign(a);
      unassign(b);
    }
  }

  /**
   * @param plan Plan to execute.
   */
  void balance(final RegionPlan plan) {
    synchronized (this.regionPlans) {
      this.regionPlans.put(plan.getRegionName(), plan);
    }
    unassign(plan.getRegionInfo(), false, plan.getDestination());
  }

  public void stop() {
    this.timeoutMonitor.interrupt();
    this.timerUpdater.interrupt();
  }

  /**
   * Check whether the RegionServer is online.
   * @param serverName
   * @return True if online.
   */
  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && this.serverManager.isServerOnline(serverName);
  }
  /**
   * Shutdown the threadpool executor service
   */
  public void shutdown() {
    if (null != threadPoolExecutorService) {
      this.threadPoolExecutorService.shutdown();
    }
  }

  protected void setEnabledTable(String tableName) {
    try {
      this.zkTable.setEnabledTable(tableName);
    } catch (KeeperException e) {
      // here we can abort as it is the start up flow
      String errorMsg = "Unable to ensure that the table " + tableName
          + " will be" + " enabled because of a ZooKeeper issue";
      LOG.error(errorMsg);
      this.master.abort(errorMsg, e);
    }
  }
}
