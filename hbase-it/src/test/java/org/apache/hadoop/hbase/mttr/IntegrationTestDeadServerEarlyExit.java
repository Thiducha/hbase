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

package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.eclipse.jdt.internal.core.Assert;

import java.io.IOException;

public class IntegrationTestDeadServerEarlyExit extends AbstractIntegrationTestRecovery {
  private int zkTimeout;

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }

  Configuration conf;
  HConnection hc;
  HTable h;

  byte[] row = "myrow".getBytes();


  @Override
  protected void beforeStart() {
    // Do a copy, we will create w connection for ourselves only
    conf = new Configuration(util.getConfiguration());

    zkTimeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);

    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, zkTimeout * 3);
  }

  @Override
  protected void beforeKill() throws Exception {

    // We create a connection that we will use after the kill. This connection will contain
    //  a socket to the dead server, this way we will have a read timeout and not a connect timeout
    // (the connect timeout beeing usually much smaller than the read one).
    hc = HConnectionManager.getConnection(conf);
    h = new HTable(conf, TABLE_NAME);

    Put p = new Put(row);
    p.add(AbstractIntegrationTestRecovery.COLUMN_NAME.getBytes(), row, row);

    h.put(p);
  }

  long endGetTime;

  @Override
  protected void afterKill() throws IOException {
    long startGetTime = System.currentTimeMillis();


    // We're doing a get on the dead server. Despite the long rpc timeout, we expect
    //  to be informed of the failure just after the ZK timeout and then go to the right server
    //  instead of going to the dead one.

    Get g = new Get(row);
    Object o = h.get(g);
    Assert.isNotNull(o);

    // zookeeper.session.timeout 180000 -> recommended 60000
    // hbase.client.operation.timeout - see https://reviews.apache.org/r/755/    - Integer.MAX
    // hbase.rpc.timeout
    // hbase.client.scanner.timeout.period

    // The server has been unplugged, but we don't know yet.
    // So we will wait until: the get timeout OR the server is marked as dead.

    endGetTime = System.currentTimeMillis();

    long getTime = (endGetTime - startGetTime);

    Assert.isTrue(getTime <  zkTimeout + 30000);
  }
}
