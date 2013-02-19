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


package org.apache.hadoop.hbase.master;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterStatusPublisher extends Chore {
  private long lastMessageTime = 0;
  private final HMaster master;
  private DatagramChannel channel;
  private final AtomicBoolean connected = new AtomicBoolean(false);
  private final int messagePeriod; // time between two message
  private final ConcurrentMap<ServerName, Integer> lastSent =
      new ConcurrentHashMap<ServerName, Integer>();

  /**
   * We want to limit the size of the protobuf message sent, do fit into a single packet.
   * a reasonable size ofr ip / ethernet is less than 1Kb.
   */
  public static int MAX_SERVER_PER_MESSAGE = 10;

  /**
   * If a server dies, we're sending the information multiple times in case a receiver misses the
   * message.
   */
  public static int NB_SEND = 5;

  public ClusterStatusPublisher(HMaster master, Configuration conf)
      throws UnknownHostException {
    super("HBase clusterStatusPublisher for " + master.getName(),
        conf.getInt(HConstants.STATUS_MULTICAST_PERIOD,
            HConstants.DEFAULT_STATUS_MULTICAST_PERIOD), master);
    this.master = master;
    this.messagePeriod = conf.getInt(HConstants.STATUS_MULTICAST_PERIOD,
        HConstants.DEFAULT_STATUS_MULTICAST_PERIOD);
    connect(conf);
  }

  // for tests
  protected ClusterStatusPublisher(){
    messagePeriod  = 0;
    master = null;
  }


  private void connect(Configuration conf) throws UnknownHostException {
    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
        HConstants.DEFAULT_STATUS_MULTICAST_PORT);

    // Can't be NiO with Netty today => not implemented in Netty.
    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipeline(Channels.pipeline(new ProtobufEncoder()));


    channel = (DatagramChannel) b.bind(new InetSocketAddress(0));
    channel.getConfig().setReuseAddress(true);

    InetAddress ina = InetAddress.getByName(mcAddress);
    channel.joinGroup(ina);
    channel.connect(new InetSocketAddress(mcAddress, port));

    connected.set(true);
  }


  @Override
  protected void chore() {
    if (!connected.get()) {
      return;
    }

    List<ServerName> sns = generateDeadServersListToSend();
    if (sns.isEmpty()) {
      // Nothing to send. Done.
      return;
    }

    final long curTime = EnvironmentEdgeManager.currentTimeMillis();
    if (lastMessageTime > curTime - messagePeriod) {
      // We already sent something less than 10 second ago. Done.
      return;
    }

    // Ok, we're going to send something then.
    lastMessageTime = curTime;

    // We're reusing an existing protobuf message, but we don't send everything.
    // This could be extended in the future, for example if we want to send stuff like the
    //  META server name.
    ClusterStatus cs = new ClusterStatus(VersionInfo.getVersion(),
        master.getMasterFileSystem().getClusterId().toString(),
        null,
        sns,
        master.getServerName(),
        null,
        null,
        null,
        null);

    ClusterStatusProtos.ClusterStatus csp = cs.convert();

    channel.write(csp);
  }

  protected void cleanup() {
    if (channel != null) {
      channel.close();
    }
    connected.set(false);
  }

  /**
   * Create the dead server to send. A dead server is sent NB_SEND times. We send at max
   *  MAX_SERVER_PER_MESSAGE at a time. if there are too many dead servers, we send the newly
   *  dead first.
   */
  protected List<ServerName> generateDeadServersListToSend() {
    // We're getting the message sent since last time, and add them to the list
    long since = EnvironmentEdgeManager.currentTimeMillis() - messagePeriod * 2;
    for (Pair<ServerName, Long> dead : getDeadServers(since)) {
      lastSent.putIfAbsent(dead.getFirst(), 0);
    }

    // We're sending the new deads first.
    List<Map.Entry<ServerName, Integer>> entries = new ArrayList<Map.Entry<ServerName, Integer>>();
    entries.addAll(lastSent.entrySet());
    Collections.sort(entries, new Comparator<Map.Entry<ServerName, Integer>>() {
      @Override
      public int compare(Map.Entry<ServerName, Integer> o1, Map.Entry<ServerName, Integer> o2) {
        return o1.getValue().compareTo(o2.getValue());
      }
    });

    // With a limit of MAX_SERVER_PER_MESSAGE
    int max = entries.size() > MAX_SERVER_PER_MESSAGE ? MAX_SERVER_PER_MESSAGE : entries.size();
    List<ServerName> res = new ArrayList<ServerName>(max);

    for (int i = 0; i < max; i++) {
      Map.Entry<ServerName, Integer> toSend = entries.get(i);
      if (toSend.getValue() >= (NB_SEND -1)) {
        lastSent.remove(toSend.getKey());
      } else {
        lastSent.replace(toSend.getKey(), toSend.getValue(), toSend.getValue() + 1);
      }

      res.add(toSend.getKey());
    }

    return res;
  }

  /**
   * Get the servers dies since a given timestamp.
   * protected because it can be subclassed by the tests.
   */
  protected List<Pair<ServerName, Long>> getDeadServers(long since){
    if (master.getServerManager() == null){
      return Collections.emptyList();
    }

    return master.getServerManager().getDeadServers().copyDeadServersSince(since);
  }
}
