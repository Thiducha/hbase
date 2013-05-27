package org.apache.hadoop.hbase.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Category(SmallTests.class)
public class TestAsyncProcess {
  private static final byte[] DUMMY_TABLE = "DUMMY_TABLE".getBytes();
  private static final byte[] DUMMY_BYTES_1 = "DUMMY_BYTES_1".getBytes();
  private static final byte[] DUMMY_BYTES_2 = "DUMMY_BYTES_2".getBytes();
  private static final byte[] FAILS = "FAILS".getBytes();
  private Configuration conf = new Configuration();


  private static ServerName sn = new ServerName("locahost:10,1254");
  private static HRegionInfo hri1 = new HRegionInfo(DUMMY_BYTES_1);
  private static HRegionInfo hri2 = new HRegionInfo(DUMMY_BYTES_1);
  private static HRegionLocation loc1 = new HRegionLocation(hri1, sn);
  private static HRegionLocation loc2 = new HRegionLocation(hri2, sn);

  private static final String success = "success";
  private static Exception failure = new Exception("failure");

  static class MyAsyncProcess<Res> extends AsyncProcess<Res> {

    public MyAsyncProcess(HConnection hc,
                          AsyncProcessCallback<Res> callback, Configuration conf) {
      super(hc, DUMMY_TABLE, new ThreadPoolExecutor(1, 10, 60, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(),
          Threads.newDaemonThreadFactory("test-TestAsyncProcess"))
          , callback, conf);
    }

    /**
     * Do not call a server, fails if the rowkey of the operation is{@link #FAILS}
     */
    @Override
    protected <R> Callable<MultiResponse> createCallable(final HConnection connection,
                                                         final HRegionLocation loc,
                                                         final MultiAction<R> multi,
                                                         final byte[] tableName) {

      final MultiResponse mr = new MultiResponse();
      for (Map.Entry<byte[], List<Action<R>>> entry : multi.actions.entrySet()) {
        for (Action a : entry.getValue()) {
          if (Arrays.equals(FAILS, a.getAction().getRow())) {
            mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), failure);
          } else {
            mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), success);
          }
        }
      }

      return new Callable<MultiResponse>() {
        @Override
        public MultiResponse call() throws Exception {
          return mr;
        }
      };
    }
  }

  @Test
  public void testSubmit() throws Exception {
    HConnection hc = createHConnection();

    AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(true, true));

    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testSubmitWithCB() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();

    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(true, true));

    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());

    while (!(mcb.successCalled.get() == 1) && !ap.hasError()) {
      Thread.sleep(1);
    }
    Assert.assertEquals(mcb.successCalled.get(), 1);
  }

  @Test
  public void testSubmitBusyRegion() throws Exception {
    HConnection hc = createHConnection();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(true, true));

    ap.incTaskCounters(hri1.getEncodedName());
    ap.submit(puts);
    Assert.assertEquals(puts.size(), 1);

    ap.decTaskCounters(hri1.getEncodedName());
    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());
  }

  @Test
  public void testFail() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    Put p =  createPut(true, false);
    puts.add(p);

    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());

    while (!ap.hasError()) {
      Thread.sleep(1);
    }

    Assert.assertEquals(0, mcb.successCalled.get());
    Assert.assertEquals(2, mcb.retriableFailure.get());
    Assert.assertEquals(1, mcb.failureCalled.get());

    Assert.assertEquals(1, ap.getErrors().exceptions.size());
    Assert.assertTrue("was: " + ap.getErrors().exceptions.get(0),
        failure.equals(ap.getErrors().exceptions.get(0)));
    Assert.assertTrue("was: " + ap.getErrors().exceptions.get(0),
        failure.equals(ap.getErrors().exceptions.get(0)));

    Assert.assertEquals(1, ap.getFailedOperations().size());
    Assert.assertTrue("was: " + ap.getFailedOperations().get(0),
        p.equals(ap.getFailedOperations().get(0)));
  }

  @Test
  public void testFailAndSuccess() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(true, false));
    puts.add(createPut(true, true));
    puts.add(createPut(true, true));

    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());

    while (!ap.hasError()) {
      Thread.sleep(1);
    }
    Assert.assertEquals(mcb.successCalled.get(), 2);
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    Assert.assertEquals(1, ap.getErrors().actions.size());


    puts.add(createPut(true, true));
    ap.submit(puts);
    Assert.assertTrue(puts.isEmpty());

    while (mcb.successCalled.get() != 3) {
      Thread.sleep(1);
    }
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    ap.clearErrors();
    Assert.assertTrue(ap.getErrors().actions.isEmpty());
  }

  @Test
  public void testFlush() throws Exception {
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB();
    AsyncProcess ap = new MyAsyncProcess<Object>(hc, mcb, conf);

    List<Put> puts = new ArrayList<Put>();
    puts.add(createPut(true, false));
    puts.add(createPut(true, true));
    puts.add(createPut(true, true));

    ap.submit(puts);
    ap.waitUntilDone();

    Assert.assertEquals(mcb.successCalled.get(), 2);
    Assert.assertEquals(mcb.retriableFailure.get(), 2);
    Assert.assertEquals(mcb.failureCalled.get(), 1);

    Assert.assertEquals(1, ap.getFailedOperations().size());
  }


  private class MyCB implements AsyncProcess.AsyncProcessCallback<Object> {
    private AtomicInteger successCalled = new AtomicInteger(0);
    private AtomicInteger failureCalled = new AtomicInteger(0);
    private AtomicInteger retriableFailure = new AtomicInteger(0);


    @Override
    public void success(int originalIndex, byte[] region, byte[] row, Object o) {
      successCalled.incrementAndGet();
    }

    @Override
    public void failure(int originalIndex, byte[] region, byte[] row, Throwable t) {
      failureCalled.incrementAndGet();
    }

    @Override
    public boolean retriableFailure(int originalIndex, Row row, byte[] region, Throwable exception) {
      // We retry twice only.
      return (retriableFailure.incrementAndGet() < 2);
    }
  }


  private static HConnection createHConnection() throws Exception {
    HConnection hc = Mockito.mock(HConnection.class);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_1), Mockito.anyBoolean())).thenReturn(loc1);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_1))).thenReturn(loc1);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_2), Mockito.anyBoolean())).thenReturn(loc2);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(DUMMY_BYTES_2))).thenReturn(loc2);

    Mockito.when(hc.getRegionLocation(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(FAILS), Mockito.anyBoolean())).thenReturn(loc2);
    Mockito.when(hc.locateRegion(Mockito.eq(DUMMY_TABLE),
        Mockito.eq(FAILS))).thenReturn(loc2);

    return hc;
  }


  /**
   * @param reg1 if true, creates a put on region 1, region 2 otherwise
   * @param success if true, the put will succeed.
   * @return a put
   */
  private Put createPut(boolean reg1, boolean success) {
    Put p;
    if (!success) {
      p = new Put(FAILS);
    } else if (reg1) {
      p = new Put(DUMMY_BYTES_1);
    } else {
      p = new Put(DUMMY_BYTES_2);
    }

    p.add(DUMMY_BYTES_1, DUMMY_BYTES_1, DUMMY_BYTES_1);

    return p;
  }
}
