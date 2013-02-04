package org.apache.hadoop.hbase.mttr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.eclipse.jdt.internal.core.Assert;

import java.io.IOException;

public class IntegrationTestDeadServerEarlyExit extends AbstractIntegrationTestRecovery {

  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.unplug(willDieBox);
  }

  Configuration conf;
  HConnection hc;
  HTable h;

  byte[] row = "myrow".getBytes();


  @Override
  protected void beforeStart(){
    // Do a copy, we will create w connection for ourselves only
    conf = new Configuration(util.getConfiguration());
    conf.setInt("hbase.client.operation.timeout", 600000);
    conf.setInt("hbase.rpc.timeout", 500000);
  }

  @Override
  protected void beforeKill() throws Exception {

    // We create a connection.
    hc = HConnectionManager.getConnection(conf);
    h = new HTable(conf, TABLE_NAME);


    Put p = new Put(row);
    p.add(  AbstractIntegrationTestRecovery.COLUMN_NAME.getBytes() , row, row);

    h.put(p);
  }

  long endGetTime;

  @Override
  protected void afterKill() throws IOException {
    long startGetTime = System.currentTimeMillis();
    Get g = new Get(row);
    Object o = h.get(g);
    Assert.isNotNull(o);

    // zookeeper.session.timeout 180000 -> recommended 60000
    // hbase.client.operation.timeout - see https://reviews.apache.org/r/755/
    // hbase.rpc.timeout
    // hbase.client.scanner.timeout.period

    // The server has been unplugged, but we don't know yet.
    // So we will wait until: the get timeout OR the server is marked as dead.

    endGetTime = System.currentTimeMillis();
    System.out.println("************************************ get time = "+ (endGetTime - startGetTime));
  }

  @Override
  protected void afterDetection() {
    long detectTime = System.currentTimeMillis();
    Assert.isTrue((detectTime - endGetTime) < 10000);
  }
}
