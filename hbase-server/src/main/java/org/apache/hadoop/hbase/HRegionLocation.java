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
package org.apache.hadoop.hbase;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Addressing;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Data structure to hold HRegionInfo and the address for the hosting
 * HRegionServer.  Immutable.  Comparable, but we compare the 'location' only:
 * i.e. the hostname and port, and *not* the regioninfo.  This means two
 * instances are the same if they refer to the same 'location' (the same
 * hostname and port), though they may be carrying different regions.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HRegionLocation implements Comparable<HRegionLocation> {
  private final HRegionInfo regionInfo;
  private final ServerName servername;
  private final long seqNum;
  // Cache of the 'toString' result.
  private String cachedString = null;
  // Cache of the hostname + port
  private String cachedHostnamePort;


  /**
   * Test constructor w/o seqNum.
   */
  public HRegionLocation(HRegionInfo regionInfo, ServerName servername) {
    this(regionInfo, servername, 0);
  }

  public HRegionLocation(HRegionInfo regionInfo, ServerName servername, long seqNum) {
    this.regionInfo = regionInfo;
    this.servername = servername;
    this.seqNum = seqNum;
  }

  @Deprecated // for tests
  public HRegionLocation(HRegionInfo regionInfo, String hostname, int port) {
    this (regionInfo, new ServerName(hostname, port, 0),0 );
  }

  public HRegionLocation(HRegionInfo regionInfo, String hostname, int port, long seq) {
    this (regionInfo, new ServerName(hostname, port, 0),seq );
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public synchronized String toString() {
    if (this.cachedString == null) {
      this.cachedString = "region=" + this.regionInfo.getRegionNameAsString() +
      ", hostname=" + this.getHostname() + ", port=" + this.getPort()
      + ", seqNum=" + seqNum;
    }
    return this.cachedString;
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionLocation)) {
      return false;
    }
    return this.compareTo((HRegionLocation)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.servername.getHostname().hashCode();
    result ^= this.getPort();
    return result;
  }

  /** @return HRegionInfo */
  public HRegionInfo getRegionInfo(){
    return regionInfo;
  }

  public String getHostname() {
    return this.servername.getHostname();
  }

  public int getPort() {
    return this.servername.getPort();
  }

  public long getSeqNum() {
    return seqNum;
  }

  /**
   * @return String made of hostname and port formatted as per {@link Addressing#createHostAndPortStr(String, int)}
   */
  public synchronized String getHostnamePort() {
    if (this.cachedHostnamePort == null) {
      this.cachedHostnamePort =
        Addressing.createHostAndPortStr(this.getHostname(), this.getPort());
    }
    return this.cachedHostnamePort;
  }

  //
  // Comparable
  //

  public int compareTo(HRegionLocation o) {
    int result = this.getHostname().compareTo(o.getHostname());
    if (result != 0) return result;
    return this.getPort() - o.getPort();
  }

  public ServerName getServerName() {
    return servername;
  }
}
