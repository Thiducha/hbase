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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Do a set of puts, unplug the RS. HBase will need to revover the data, using the distributed split
 *  algorithms.
 *
 * Time taken ~5 seconds.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryRSLossBetweenInserts extends AbstractIntegrationTestRecovery {

  HTable h = null;

  @Override
  protected void beforeKill() throws Exception {
    h = new HTable(util.getConfiguration(), tableName);

    for (HRegionInfo hri : h.getRegionLocations().keySet()){
      byte[] key = hri.getStartKey();
      if (key != null && key.length > 0){
        Put p = new Put(key);
        p.add(CF.getBytes(), key, key);
        h.put(p);
      }
    }
  }

  @Override
  protected void afterRecovery() throws Exception{
    for (HRegionInfo hri : h.getRegionLocations().keySet()){
      byte[] key = hri.getStartKey();
      if (key != null && key.length > 0){
        Get g = new Get(key);
        Result res = h.get(g);
        Assert.assertTrue(res.size() == 1);
      }
    }

    h.close();
  }

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }

  @Override
  protected void validate(long failureDetectedTime, long failureFixedTime) throws IOException {
    int zkTimeout = util.getConfiguration().getInt(
        HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);

    performanceChecker.logAndCheck(failureDetectedTime, zkTimeout + getMttrSmallTime());
    performanceChecker.logAndCheck(failureFixedTime, getMttrSmallTime());
  }
}
