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

package org.apache.hadoop.hbase.ipc;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseRecoveryTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Category(LargeTests.class)
public class Test_HBASE_6364 {
  private static final Log LOG = LogFactory.getLog(Test_HBASE_6364.class);

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.ALL);
    Logger.getLogger(DFSClient.class).setLevel(Level.DEBUG);
  }

  private HBaseRecoveryTestingUtility hrtu = new HBaseRecoveryTestingUtility();

  @Test
  public void test_6364() throws Exception {
    LOG.info("Start");

    hrtu.startClusterSynchronous(1, 1);
    hrtu.startNewRegionServer();
    hrtu.moveTableTo(".META.", 1); // We will have only meta on this server

    hrtu.createTable(10, 0);

    HBaseClient.sleep =
        hrtu.getHBaseCluster().getRegionServer(1).getRpcServer().getListenerAddress().getPort();
    hrtu.stopDirtyRegionServer(1);

    final long start = System.currentTimeMillis();

    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicInteger errors = new AtomicInteger(0);

    final int nbTest = 20;
    for (int i=0; i<nbTest; i++){
      Thread t = new Thread(){
        public void run(){
          try {
            HTable h = new HTable(hrtu.getConfiguration(), hrtu.getTestTableName());
            h.get(new Get(HConstants.EMPTY_START_ROW)) ;
            h.close();
          } catch (IOException e) {
            errors.incrementAndGet();
          } finally {
            counter.incrementAndGet();
          }
        }
      } ;
      t.start();
    }

    while(counter.get() < nbTest){ Thread.sleep(1); }

    LOG.info("Time: " + (System.currentTimeMillis()-start) + " nb errors: "+errors.get());
    HBaseClient.sleep = 0;
    hrtu.stopCleanCluster();
    LOG.info("Done");
  }
}