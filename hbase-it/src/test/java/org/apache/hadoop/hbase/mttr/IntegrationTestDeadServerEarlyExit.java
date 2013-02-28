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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;

import java.io.IOException;

/**
 * Test the fix for HBASE-7590: we expect the client to see immediately that the region server
 *  is dead because of the notification from the master.
 */
public class IntegrationTestDeadServerEarlyExit extends AbstractIntegrationTestRecovery {
  private int zkTimeout;
  private Configuration conf;
  private HTable h;
  private final byte[] row = "myrow".getBytes();


  @Override
  protected void kill(String hostname) throws Exception {
    hcm.unplug(hostname);
  }



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
    // (the connect timeout being usually much smaller than the read one).
    Assert.assertNotNull(HConnectionManager.getConnection(conf)) ;
    h = new HTable(conf, tableName);

    Put p = new Put(row);
    p.add(AbstractIntegrationTestRecovery.COLUMN_NAME.getBytes(), row, row);

    h.put(p);
  }


  @Override
  protected void afterKill() throws IOException {
    long startGetTime = System.currentTimeMillis();


    // We're doing a get on the dead server. Despite the long rpc timeout, we expect
    //  to be informed of the failure just after the ZK timeout and then go to the right server
    //  instead of going to the dead one.

    Get g = new Get(row);
    Object o = h.get(g);
    Assert.assertNotNull(o);

    // The server has been unplugged, but we don't know yet.
    // So we will wait until: the get timeout OR the server is marked as dead.

    long endGetTime = System.currentTimeMillis();

    long getTime = (endGetTime - startGetTime);

    // We didn't wait for the socket timeout
    Assert.assertTrue(getTime < zkTimeout + getMttrSmallTime());

  }
}
