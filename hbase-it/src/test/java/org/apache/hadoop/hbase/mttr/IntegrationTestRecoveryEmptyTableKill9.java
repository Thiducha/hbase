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
import org.junit.experimental.categories.Category;

/**
 * On a kill 9, we cannot rely on the shutdown hooks. On 0.94, we rely on the ZK timeout. On 0.96,
 * the scripts include a mechanism to cleanup the ZK status when the regionserver or the master
 * dies. This allows an immediate recovery.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableKill9
    extends AbstractIntegrationTestRecovery {

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.signal(ClusterManager.ServiceType.HBASE_REGIONSERVER, "KILL", willDieBox);
  }

  @Override
  protected void validate(long failureDetectedTime, long failureFixedTime) {
    performanceChecker.logAndCheck(failureDetectedTime, getMttrSmallTime());
    performanceChecker.logAndCheck(failureFixedTime, getMttrSmallTime());
  }
}
