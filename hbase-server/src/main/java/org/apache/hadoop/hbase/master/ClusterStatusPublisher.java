package org.apache.hadoop.hbase.master;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.ClusterStatusListener;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
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
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterStatusPublisher extends Chore {
  private String mcAddress;
  private int port;
  private long lastMessageTime = 0;
  private HMaster master;
  private DatagramChannel channel;
  private volatile AtomicBoolean connected = new AtomicBoolean(false);


  public ClusterStatusPublisher(HMaster master, int p, Configuration conf) throws UnknownHostException {
    super("HBase clusterStatusPublisher for " + master.getName(), p, master);
    this.master = master;
    connect(conf);
  }


  public void connect(Configuration conf) throws UnknownHostException {
    mcAddress = conf.get(ClusterStatusListener.HBASE_STATUS_MULTICAST_ADDRESS, "226.1.1.3");
    port = conf.getInt(ClusterStatusListener.HBASE_STATUS_MULTICAST_PORT, 60045);

    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new ProtobufEncoder(), new ProtobufDecoder(ClusterStatusProtos.ClusterStatus.getDefaultInstance()), null);
      }
    });

    b.setOption("reuseAddress", true);
    b.setOption("receivedBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024));

    channel = (DatagramChannel) b.bind(new InetSocketAddress(0));

    InetAddress ina = InetAddress.getByName(mcAddress);
    channel.joinGroup(ina);
    channel.connect(new InetSocketAddress(mcAddress, port));

    connected.set(true);
  }

  /**
   * We're sending:
   * - at max one message every 10 seconds
   * - at min one message every 30 seconds (except if there is nothing to send)
   * - immediately if there is a new dead server.
   */
  @Override
  protected void chore() {
    if (!connected.get()) {
      return;
    }

    final long curTime = EnvironmentEdgeManager.currentTimeMillis();
    final long cutOff = curTime - 30000;
    ClusterStatus cs = new ClusterStatus(VersionInfo.getVersion(),
        master.getMasterFileSystem().getClusterId().toString(),
        Collections.EMPTY_MAP,
        master.getServerManager().getDeadServers(),
        master.getServerName(),
        Collections.EMPTY_LIST,
        Collections.EMPTY_MAP,
        new String[]{},
        false); // don't know

    ClusterStatusProtos.ClusterStatus csp = cs.convert();

    channel.write(csp);
  }

  protected void cleanup() {
    if (channel != null) {
      channel.close();
    }
  }

}
