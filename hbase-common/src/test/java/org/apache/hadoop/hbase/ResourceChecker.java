/*
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

package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;


public class ResourceChecker {
  private static final Log LOG = LogFactory.getLog(ResourceChecker.class);
  private String tagLine;

  public ResourceChecker(String tagLine) {
    this.tagLine = tagLine;
  }


  abstract static class ResourceAnalyzer {
    /**
     * Maximum we set for the resource. Will get a warning in logs
     * if we go other this limit.
     */
    public int getMax() {
      return Integer.MAX_VALUE;
    }

    public int getMin() {
      return Integer.MIN_VALUE;
    }

    public String getName() {
      String className = this.getClass().getSimpleName();
      final String extName = ResourceAnalyzer.class.getSimpleName();
      if (className.endsWith(extName)) {
        return className.substring(className.length() - extName.length());
      } else {
        return className;
      }
    }

    abstract public int getVal();
  }


  private List<ResourceAnalyzer> ras = new ArrayList<ResourceAnalyzer>();
  private int[] initialValues;
  private int[] endingValues;


  private void fillInit() {
    initialValues = new int[ras.size()];
    fill(initialValues);
  }

  private void fillEndings() {
    endingValues = new int[ras.size()];
    fill(endingValues);
  }

  private void fill(int[] vals) {
    int i = 0;
    for (ResourceAnalyzer ra : ras) {
      vals[i++] = ra.getVal();
    }
  }

  public void checkInit() {
    check(initialValues);
  }

  private void checkEndings() {
    check(endingValues);
  }

  private void check(int[] vals) {
    int i = 0;
    for (ResourceAnalyzer ra : ras) {
      int cur = vals[i++];
      if (cur < ra.getMin()) {
        LOG.warn(ra.getName() + ": " + cur + " is inferior to " + ra.getMin());
      }
      if (cur > ra.getMax()) {
        LOG.warn(ra.getName() + ": " + cur + " is superior to " + ra.getMax());
      }
    }
  }

  private void logInit() {
    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (ResourceAnalyzer ra : ras) {
      int cur = initialValues[i++];
      if (sb.length() > 0) sb.append(", ");
      sb.append(ra.getName()).append("= ").append(cur);
    }
    LOG.info("before: " + tagLine + " " + sb);
  }

  private void logEndings() {
    int i = 0;
    StringBuilder sb = new StringBuilder();
    for (ResourceAnalyzer ra : ras) {
      int curP = initialValues[i];
      int curN = endingValues[i++];
      if (sb.length() > 0) sb.append(", ");
      sb.append(ra.getName()).append("= ").append(curN).append(" was (").append(curP).append(")");
      if (curN > curP) {
        sb.append(" - ").append(ra.getName()).append(" LEAK? -");
      }
    }
    LOG.info("after: " + tagLine + " " + sb);
  }


  public void start() {
    if (ras.size() == 0) {
      LOG.info("No resource analyzer");
      return;
    }
    fillInit();
    logInit();
    checkInit();
  }

  public void end() {
    if (ras.size() == 0) {
      LOG.info("No resource analyzer");
      return;
    }
    if (initialValues == null) {
      LOG.warn("No initial values");
      return;
    }

    fillEndings();
    logEndings();
    checkEndings();
  }

  public void addResourceAnalyzer(ResourceAnalyzer ra) {
    ras.add(ra);
  }
}
