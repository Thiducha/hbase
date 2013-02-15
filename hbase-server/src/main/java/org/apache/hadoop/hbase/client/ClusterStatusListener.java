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

package org.apache.hadoop.hbase.client;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;


/**
 * A class that receives the cluster status, and provide it as a set of service to the client.
 * Today, manages only the dead server list.
 * The class is abstract to allow multiple implementation, from ZooKeeper to multicast based.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
abstract public class ClusterStatusListener implements Closeable {

  /**
   * Class to be extended to manage a new dead server.
   */
  public abstract static class DeadServerHandler {

    /**
     * Called when a server is identified as dead.
     *
     * @param sn
     */
    abstract public void newDead(ServerName sn);
  }


  /**
   * Returns true if we know for sure that the server is dead.
   * @param sn the ServerName
   * @return true if the server is dead, false if it's alive or we don't know.
   */
  public abstract boolean isDead(ServerName sn);

  /**
   * Called to close the resources, if any. Cannot throw an exception.
   */
  @Override
  public void close(){}


  /**
   * An implementation using a multicast message between the master & the client.
   */
  public static class ClusterStatusMultiCastListener extends ClusterStatusListener {
    private static final Log LOG = LogFactory.getLog(ClusterStatusMultiCastListener.class);
    private DatagramChannel channel;
    private final List<ServerName> deadServers = new ArrayList<ServerName>();
    private DeadServerHandler deadServerHandler = null;

    /**
     * Check if we know if a server is dead.
     *
     * @param sn the server name to check.
     * @return true if we know for sure that the server is dead, false otherwise.
     */
    public boolean isDead(ServerName sn) {
      if (sn.getStartcode() <= 0) {
        return false;
      }

      for (ServerName dead : deadServers) {
        if (dead.getStartcode() >= sn.getStartcode() &&
            dead.getPort() == sn.getPort() &&
            dead.getHostname().equals(sn.getHostname())) {
          return true;
        }
      }

      return false;
    }


    public ClusterStatusMultiCastListener(DeadServerHandler deadServerHandler, Configuration conf)
        throws UnknownHostException, InterruptedException {
      this.deadServerHandler = deadServerHandler;
      connect(conf);
    }


    public void connect(Configuration conf) throws InterruptedException, UnknownHostException {
      DatagramChannelFactory f =
          new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

      ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
      b.setPipeline(Channels.pipeline(
          new ProtobufDecoder(ClusterStatusProtos.ClusterStatus.getDefaultInstance()),
          new ClusterStatusHandler()));

      String mcAddress = conf.get(HConstants.STATUS_MULTICAST_ADDRESS,
          HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
      int port = conf.getInt(HConstants.STATUS_MULTICAST_PORT,
          HConstants.DEFAULT_STATUS_MULTICAST_PORT);
      channel = (DatagramChannel) b.bind(new InetSocketAddress(mcAddress, port));

      channel.getConfig().setReuseAddress(true);

      InetAddress ina = InetAddress.getByName(mcAddress);
      channel.joinGroup(ina);
    }

    @Override
    public void close() {
      if (channel != null) {
        channel.close();
      }
    }


    /**
     * Class, conforming to the Netty framework, that manages the message received.s
     */
    private class ClusterStatusHandler extends SimpleChannelUpstreamHandler {

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        ClusterStatusProtos.ClusterStatus csp = (ClusterStatusProtos.ClusterStatus) e.getMessage();
        ClusterStatus ncs = ClusterStatus.convert(csp);

        if (ncs.getDeadServerNames() != null) {
          for (ServerName sn : ncs.getDeadServerNames()) {
            if (!isDead(sn)) {
              LOG.info("There is a new dead server: " + sn);
              deadServers.add(sn);
              if (deadServerHandler != null) {
                deadServerHandler.newDead(sn);
              }
            }
          }
        }
      }

      /**
       * Invoked when an exception was raised by an I/O thread or a
       * {@link org.jboss.netty.channel.ChannelHandler}.
       */
      @Override
      public void exceptionCaught(
          ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error("Unexpected exception, continuing.", e.getCause());
      }
    }
  }
}
