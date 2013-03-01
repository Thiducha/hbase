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

package org.apache.hadoop.hbase.util;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;

/**
 * Logs performance related messages. Offer a configurable assert, allowing to continue even if a
 * condition is not met.
 */
public class PerformanceChecker {
  /**
   * Field field is public as everybody can use it to log performance results
   */
  public static final Log LOG = LogFactory.getLog(PerformanceChecker.class);

  private boolean stopOnError;

  public void logAndCheck(long actual, long expected) {
    logAndCheck(null, expected, actual);
  }

  public PerformanceChecker(Configuration conf) {
    stopOnError = conf.getBoolean("hbase-it.stop.on.error", true);
  }

  public void logAndCheck(String msg, long actual, long expected) {
    boolean ok = actual <= expected;
    String fullMsg = (msg == null ? "" : msg + ", ") +
        " expected=" + expected + ", actual=" + actual + ", ok=" + ok;
    LOG.info(fullMsg);
    if (stopOnError) {
      Assert.assertTrue(msg, ok);
    }
  }

  /**
   * Log only if the condition is not met.
   */
  public void check(long actual, long expected) {
    if (actual > expected) {
      logAndCheck(null, actual, expected);
    }
  }
}
