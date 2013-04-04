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


import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MultiThreadedWriter;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

/**
 * Test the data recovery time. We're doing a kill -9, so the regions are not flushed. However, in
 *  0.96, the failure should be detected immediately. Lastly, there are only 10 regions, so we're
 *  no testing the assignment here. Measure April '13: ~180s to detect the error (i.e. zk timeout),
 *  reassign + recovery takes around 10 seconds.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryWALFewRegionsKill9 extends AbstractIntegrationTestRecovery {

  public IntegrationTestRecoveryWALFewRegionsKill9(){
    super(20);
  }

  private static class DataGenerator extends LoadTestDataGenerator {
    final private Random rd = new Random();

    public DataGenerator() {
      super(1, 1000);
    }

    @Override
    public byte[] getDeterministicUniqueKey(long keyBase) {
      return Bytes.toBytes(keyBase);
    }

    @Override
    public byte[][] getColumnFamilies() {
      return new byte[][]{CF.getBytes()};
    }

    @Override
    public byte[][] generateColumnsForCf(byte[] rowKey, byte[] cf) {
      int nb = rd.nextInt(10) + 1;
      byte[][] res = new byte[nb][];

      for (int i = 0; i < nb; i++) {
        res[i] = (rd.nextDouble() + "").getBytes();
      }

      return res;
    }

    @Override
    public byte[] generateValue(byte[] rowKey, byte[] cf, byte[] column) {
      return (rd.nextDouble() + "").getBytes();
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, Set<byte[]> columnSet) {
      return true;
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, byte[] column, byte[] value) {
      return true;
    }
  }

  @Override
  protected void beforeKill() throws IOException {
    DataGenerator dataGen = new DataGenerator();
    MultiThreadedWriter writer =
        new MultiThreadedWriter(dataGen, util.getConfiguration(), Bytes.toBytes(tableName));

    writer.setMultiPut(true);
    LOG.info("Starting data insert");
    writer.start(1L, 500000L, 5);
    writer.waitForFinish();
    LOG.info("Data inserted");
  }


  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.signal(ClusterManager.ServiceType.HBASE_REGIONSERVER, "KILL", willDieBox);
  }
}
