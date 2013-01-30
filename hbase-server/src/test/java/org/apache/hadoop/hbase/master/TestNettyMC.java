package org.apache.hadoop.hbase.master;


import org.apache.hadoop.hbase.SmallTests;
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
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

@Category(SmallTests.class)
public class TestNettyMC {

  @Test
  public void testServer() throws InterruptedException, UnknownHostException, SocketException {
    // We can't use Netty NIO here: Not implemented exception
    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new StringEncoder(CharsetUtil.ISO_8859_1), new StringDecoder(CharsetUtil.ISO_8859_1), new ClusterStatusHandler());
      }
    });

    b.setOption("broadcast", "true");
    b.setOption("reuseAddress", true);
    b.setOption("receivedBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024));

    DatagramChannel c = (DatagramChannel) b.bind(new InetSocketAddress(0));

    InetAddress ina = InetAddress.getByName("225.1.1.2");
    c.joinGroup(ina);
    c.connect(new InetSocketAddress("225.1.1.2", 8080));


    while (true) {
      c.write("Hello ");
      System.out.println("message sent");
      Thread.sleep(1000);
    }
  }

  @Test
  public void testClient() throws InterruptedException, UnknownHostException {
    DatagramChannelFactory f = new OioDatagramChannelFactory(Executors.newSingleThreadExecutor());

    ConnectionlessBootstrap b = new ConnectionlessBootstrap(f);
    b.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(new StringEncoder(CharsetUtil.UTF_8), new StringDecoder(CharsetUtil.UTF_8), new ClusterStatusHandler());
      }
    });

    b.setOption("broadcast", true);
    b.setOption("reuseAddress", true);
    b.setOption("receivedBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024));

    DatagramChannel c = (DatagramChannel) b.bind(new InetSocketAddress("225.1.1.2", 8080));
    InetAddress ina = InetAddress.getByName("225.1.1.2");
    c.joinGroup(ina);
    Thread.sleep(500000);
  }




  static class ClusterStatusHandler extends SimpleChannelUpstreamHandler {
    int i = 0;

    public void messageReceived(
        ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      String msg = (String) e.getMessage();
      System.out.println("message received:" + msg + " " + i++);
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

  public static void main(String... args) throws InterruptedException, UnknownHostException {
    new TestNettyMC().testClient();
  }
}
