package org.apache.hadoop.hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class ClusterStatusListener implements Closeable {
  public static final String HBASE_STATUS_MULTICAST_ADDRESS = "hbase.status.multicast.address.ip";
  public static final String HBASE_STATUS_MULTICAST_PORT = "hbase.status.multicast.port";
  private String mcAddress;
  private int port;
  DatagramChannel channel;
  private final AtomicReference<ClusterStatus> cs = new AtomicReference<ClusterStatus>();
  private DeadServerHandler deadServerHandler = null;

  public boolean isDead(ServerName sn) {
    if (sn.getStartcode() <= 0 || cs.get() == null || cs.get().getDeadServerNames() == null) {
      return false;
    }

    for (ServerName dead : cs.get().getDeadServerNames()) {
      if (dead.getStartcode() >= sn.getStartcode() &&
          dead.getHostAndPort().equals(sn.getHostAndPort())) {
        return true;
      }
    }

    return false;
  }

  public abstract static class DeadServerHandler {
    abstract public void newDead(ServerName sn);
  }

  public ClusterStatusListener(DeadServerHandler deadServerHandler) {
    this.deadServerHandler = deadServerHandler;
  }


  public void connect(Configuration conf) throws InterruptedException, UnknownHostException {
    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
            new ProtobufEncoder(),
            new ProtobufDecoder(ClusterStatusProtos.ClusterStatus.getDefaultInstance()), new ClusterStatusHandler(cs));
      }
    });

    mcAddress = conf.get(ClusterStatusListener.HBASE_STATUS_MULTICAST_ADDRESS, "226.1.1.3");
    port = conf.getInt(ClusterStatusListener.HBASE_STATUS_MULTICAST_PORT, 60045);


    b.setOption("reuseAddress", true);
    b.setOption("receivedBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(10024));

    DatagramChannel channel = (DatagramChannel) b.bind(new InetSocketAddress(mcAddress, port));

    InetAddress ina = InetAddress.getByName(mcAddress);
    channel.joinGroup(ina);
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.close();
    }
  }


  class ClusterStatusHandler extends SimpleChannelUpstreamHandler {
    int i = 0;
    AtomicReference<ClusterStatus> cs;

    ClusterStatusHandler(AtomicReference<ClusterStatus> cs) {
      this.cs = cs;
    }

    public void messageReceived(
        ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      ClusterStatusProtos.ClusterStatus csp = (ClusterStatusProtos.ClusterStatus) e.getMessage();
      ClusterStatus ncs = ClusterStatus.convert(csp);
      notifyNewDead(ncs);
      cs.set(ncs);
      System.out.println("message received:" + cs.get().getClusterId() + " " + i++);
    }

    private void notifyNewDead(ClusterStatus ncs) {
      if (deadServerHandler != null) {
        for (ServerName sn : ncs.getDeadServerNames()) {
          if (isDead(sn)) {
            deadServerHandler.newDead(sn);
          }
        }
      }
    }

    /**
     * Invoked when an exception was raised by an I/O thread or a
     * {@link org.jboss.netty.channel.ChannelHandler}.
     */
    public void exceptionCaught(
        ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      e.getCause().printStackTrace();
    }
  }

}
