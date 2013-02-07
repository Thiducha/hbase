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
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterStatusPublisher extends Chore {
  private long lastMessageTime = 0;
  private HMaster master;
  private DatagramChannel channel;
  private volatile AtomicBoolean connected = new AtomicBoolean(false);
  private final int messagePeriod;
  private static int MAX_SERVER_PER_MESSAGE = 10;


  public ClusterStatusPublisher(HMaster master, Configuration conf)
      throws UnknownHostException {
    super("HBase clusterStatusPublisher for " + master.getName(),
        conf.getInt(HConstants.STATUS_MULTICAST_PERIOD,
        HConstants.DEFAULT_STATUS_MULTICAST_PERIOD), master);
    this.master = master;
    this.messagePeriod =  conf.getInt(HConstants.STATUS_MULTICAST_PERIOD,
        HConstants.DEFAULT_STATUS_MULTICAST_PERIOD);
    connect(conf);
  }


  private void connect(Configuration conf) throws UnknownHostException {
    String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
    int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
        HConstants.DEFAULT_STATUS_MULTICAST_PORT);

    // Can't be NiO with Netty today => not implemented.
    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new ProtobufEncoder(),
            new ProtobufDecoder(ClusterStatusProtos.ClusterStatus.getDefaultInstance()), null);
      }
    });

    b.setOption("reuseAddress", true);
    b.setOption("receivedBufferSizePredictorFactory",
        new FixedReceiveBufferSizePredictorFactory(1024));

    channel = (DatagramChannel) b.bind(new InetSocketAddress(0));

    InetAddress ina = InetAddress.getByName(mcAddress);
    channel.joinGroup(ina);
    channel.connect(new InetSocketAddress(mcAddress, port));

    connected.set(true);
  }

  /**
   * We're sending:
   * - at max one message every 10 seconds
   * - immediately if there is a new dead server and the
   * previous message is more than 10 seconds ago.
   */
  @Override
  protected void chore() {
    if (!connected.get()) {
      return;
    }

    List<ServerName> sns = getLastDeadServers();
    if (sns.isEmpty()){
      // Nothing to send. Done.
      return;
    }

    final long curTime = EnvironmentEdgeManager.currentTimeMillis();
    if (lastMessageTime > curTime - messagePeriod){
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
        getLastDeadServers(),
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

  private List<ServerName> getLastDeadServers(){
    long since = EnvironmentEdgeManager.currentTimeMillis() - messagePeriod * 3;
    List<Pair<ServerName, Long>> deads =
        master.getServerManager().getDeadServers().copyDeadServersSince(since);

    List<ServerName> res = new ArrayList<ServerName>(MAX_SERVER_PER_MESSAGE);

    int max = deads.size() > MAX_SERVER_PER_MESSAGE ? MAX_SERVER_PER_MESSAGE : deads.size();
    for (int i=0; i< max; i++){
      res.add( deads.get(i).getFirst() );
    }

    return res;
  }

}
