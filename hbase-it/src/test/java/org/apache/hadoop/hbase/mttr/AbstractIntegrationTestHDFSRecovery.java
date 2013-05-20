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

package org.apache.hadoop.hbase.mttr;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.Test;

/**
 * An integration test to measure the time needed to recover when we lose a datanode.
 * Makes sense only with a distributed cluster.
 * <p/>
 * Scenario:
 * - start 3 boxes
 * - create a table
 * - have all regions of this table on a single one
 * - start another datanode
 * - kill one datanode
 * - measure time.
 * <p></p>
 */

public abstract class AbstractIntegrationTestHDFSRecovery extends AbstractIntegrationTestRecovery {
  protected static final Log LOG
      = LogFactory.getLog(AbstractIntegrationTestHDFSRecovery.class);

  protected int nbDN;

  /**
   * Default constructor, creates a table with 10 regions.
   */
  protected AbstractIntegrationTestHDFSRecovery() {
    super();
  }

  /**
   * Do the following tasks, in this order:
   * start the cluster, with:
   * main box: 1 ZooKeeper, 1 Namenode, 1 Master, 1 Region Server
   * willDieBox: 1 datanode
   * willSurviveBox: 1 datanode
   * lateBox: 1 datanode
   */
  /**
   * Start a namenode and 3 datanodes.
   */
  @Override
  protected void genericStart() throws Exception {
    // Let's start ZK immediately, it will initialize itself while the NN and the DN are starting
    hcm.start(ClusterManager.ServiceType.ZOOKEEPER, mainBox);

    if (destructiveTest) {
      hcm.formatNameNode(mainBox); // synchronous
    }

    hcm.start(ClusterManager.ServiceType.HADOOP_NAMENODE, mainBox);
    dhc.waitForNamenodeAvailable();

    // Number of datanode
    nbDN = 3;
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willSurviveBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, lateBox);
    hcm.start(ClusterManager.ServiceType.HADOOP_DATANODE, willDieBox);
    dhc.waitForDatanodesRegistered(nbDN);

    hcm.start(ClusterManager.ServiceType.HBASE_MASTER, mainBox);

    // There is no datanode on the main box. This way, when doing the recovery, all the reads will
    //  be done on a remote box. If the stale mode is not active, it will trigger socket timeouts.
    hcm.start(ClusterManager.ServiceType.HBASE_REGIONSERVER, mainBox);
    // We want meta & root on the main server, so we start only one RS at the beginning

    while (!dhc.waitForActiveAndReadyMaster() ||
        util.getHBaseAdmin().getClusterStatus().getRegionsCount() != 1) {
      Thread.sleep(200);
    }

    // No balance please
    util.getHBaseAdmin().setBalancerRunning(false, true);
  }

  @Test
  @Override
  public void testKillServer() throws Exception {
    prepareCluster();
    beforeStart();
    genericStart();
    Configuration localConf = new Configuration(util.getConfiguration());

    createTable();
    localConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HTable htable = new HTable(localConf, tableName);

    beforeKill();

    final int nbReg = 1 + this.regionCount;
    long lastLog = System.currentTimeMillis();
    while (nbReg != dhc.getClusterStatus().getRegionsCount()) {
      if (lastLog + 10000 < System.currentTimeMillis()) {
        lastLog = System.currentTimeMillis();
        LOG.info("Waiting: we want the cluster status to contain " + nbReg +
            " regions, it contains " + dhc.getClusterStatus().getRegionsCount() + " regions");
      }
      Thread.sleep(400);
    }

    kill(willDieBox);

    try {
      int dnport = Integer.parseInt(util.getConfiguration().get("dfs.datanode.address", "0.0.0.0:50010").split(":")[1]);
      // Check that the DN is really killed. We do that by trying to connect to the server with
      // a minimum connect timeout. If it succeeds, it means it's still there...
      lastLog = System.currentTimeMillis();
      while (ClusterManager.isReachablePort(willDieBox, dnport)) {
        if (lastLog + 10000 < System.currentTimeMillis()) {
          lastLog = System.currentTimeMillis();
          LOG.info("Waiting: we can still reach the port " + dnport + " on " + willDieBox);
        }
        Thread.sleep(400);
      }

      afterKill();

      afterRecovery();
    } finally {
      // If it was an unplug, we replug it now
      htable.close();
      hcm.replug(willDieBox);
    }

    if (!destructiveTest) {
      util.getHBaseAdmin().deleteTable(tableName);
    }
  }
}
