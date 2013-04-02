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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.DeadServer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.zookeeper.KeeperException;

/**
 * Shutdown handler for the server hosting <code>.META.</code>
 */
@InterfaceAudience.Private
public class MetaServerShutdownHandler extends ServerShutdownHandler {
  private static final Log LOG = LogFactory.getLog(MetaServerShutdownHandler.class);
  public MetaServerShutdownHandler(final Server server,
      final MasterServices services,
      final DeadServer deadServers, final ServerName serverName) {
    super(server, services, deadServers, serverName,
      EventType.M_META_SERVER_SHUTDOWN, true);
  }

  @Override
  public void process() throws IOException {
    boolean gotException = true; 
    try{
      try {
        LOG.info("Splitting META logs for " + serverName);
        if (this.shouldSplitHlog) {
          this.services.getMasterFileSystem().splitMetaLog(serverName);
        }
      } catch (IOException ioe) {
        this.services.getExecutorService().submit(this);
        this.deadServers.add(serverName);
        throw new IOException("failed log splitting for " +
            serverName + ", will retry", ioe);
      }
  
      // Assign meta if we were carrying it.
      // Check again: region may be assigned to other where because of RIT
      // timeout
      if (this.services.getAssignmentManager().isCarryingMeta(serverName)) {
        LOG.info("Server " + serverName + " was carrying META. Trying to assign.");
        this.services.getAssignmentManager().regionOffline(HRegionInfo.FIRST_META_REGIONINFO);
        verifyAndAssignMetaWithRetries();
      } else {
        LOG.info("META has been assigned to otherwhere, skip assigning.");
      }
      
      gotException = false;
    } finally {
      if (gotException){
        // If we had an exception, this.deadServers.finish will be skipped in super.process()
        this.deadServers.finish(serverName);
      }     
    }
    super.process();
  }

  /**
   * Before assign the META region, ensure it haven't
   *  been assigned by other place
   * <p>
   * Under some scenarios, the META region can be opened twice, so it seemed online
   * in two regionserver at the same time.
   * If the META region has been assigned, so the operation can be canceled.
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private void verifyAndAssignMeta()
      throws InterruptedException, IOException, KeeperException {
    long timeout = this.server.getConfiguration().
        getLong("hbase.catalog.verification.timeout", 1000);
    if (!this.server.getCatalogTracker().verifyMetaRegionLocation(timeout)) {
      this.services.getAssignmentManager().assignMeta();
    } else if (serverName.equals(server.getCatalogTracker().getMetaLocation())) {
      throw new IOException(".META. is onlined on the dead server "
          + serverName);
    } else {
      LOG.info("Skip assigning .META., because it is online on the "
          + server.getCatalogTracker().getMetaLocation());
    }
  }

  /**
   * Failed many times, shutdown processing
   * @throws IOException
   */
  private void verifyAndAssignMetaWithRetries() throws IOException {
    int iTimes = this.server.getConfiguration().getInt(
        "hbase.catalog.verification.retries", 10);

    long waitTime = this.server.getConfiguration().getLong(
        "hbase.catalog.verification.timeout", 1000);

    int iFlag = 0;
    while (true) {
      try {
        verifyAndAssignMeta();
        break;
      } catch (KeeperException e) {
        this.server.abort("In server shutdown processing, assigning meta", e);
        throw new IOException("Aborting", e);
      } catch (Exception e) {
        if (iFlag >= iTimes) {
          this.server.abort("verifyAndAssignMeta failed after" + iTimes
              + " times retries, aborting", e);
          throw new IOException("Aborting", e);
        }
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted when is the thread sleep", e1);
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e1);
        }
        iFlag++;
      }
    }
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid();
  }
}
