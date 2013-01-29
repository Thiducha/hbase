package org.apache.hadoop.hbase.mttr;



import org.apache.hadoop.hbase.ClusterManager;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MultiThreadedWriter;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

@Category(IntegrationTests.class)
public class IntegrationTestRecoveryWALFewRegionsKill15 extends AbstractIntegrationTestRecovery {

  private static class DataGenerator extends LoadTestDataGenerator {
    Random rd = new Random();

    public DataGenerator() {
      super(1, 10000);
    }

    @Override
    public byte[] getDeterministicUniqueKey(long keyBase) {
      return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[][] getColumnFamilies() {
      return new byte[][]{COLUMN_NAME.getBytes()};
    }

    @Override
    public byte[][] generateColumnsForCf(byte[] rowKey, byte[] cf) {
      int nb = rd.nextInt(10);
      byte[][] res = new byte[nb][];


      for (int i=0; i<nb; i++){
        res[i] = (rd.nextDouble()+"").getBytes() ;

      }
      return res;
    }

    @Override
    public byte[] generateValue(byte[] rowKey, byte[] cf, byte[] column) {
      return ( rd.nextDouble()+"").getBytes() ;
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, Set<byte[]> columnSet) {
      return true;
    }

    @Override
    public boolean verify(byte[] rowKey, byte[] cf, byte[] column, byte[] value) {
      return true;
    }
  }

  protected void writeDataToWal() throws IOException {
    DataGenerator dataGen = new DataGenerator();
    MultiThreadedWriter writer =
        new MultiThreadedWriter(dataGen, util.getConfiguration() , Bytes.toBytes(TABLE_NAME));

    writer.setMultiPut(true);

    writer.start(1, 50000, 5);
    writer.waitForFinish();
  }


  @Override
  protected void kill(String willDieBox) throws Exception {
    hcm.signal(ClusterManager.ServiceType.HBASE_REGIONSERVER, "TERM", willDieBox);
  }
}
