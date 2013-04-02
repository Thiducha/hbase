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

import org.apache.hadoop.hbase.IntegrationTests;
import org.junit.experimental.categories.Category;

/**
 * When we unplug a box, we rely on the ZK timeout. So the recovery time will be roughly the
 * ZK time + split time + assignment time. Here, the table is empty and there are just a
 * small number of region, so we expect the recovery time to equals the ZK time.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRecoveryEmptyTableUnplugBox
    extends AbstractIntegrationTestRecovery {


  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }

  @Override
  protected void validate(long failureDetectedTime, long failureFixedTime) {
    performanceChecker.logAndCheck(failureFixedTime, getMttrSmallTime());
  }
}
