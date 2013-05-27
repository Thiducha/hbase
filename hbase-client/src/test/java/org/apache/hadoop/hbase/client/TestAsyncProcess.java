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

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    protected ServerCallable<MultiResponse> createCallable(
        final HRegionLocation loc, final MultiAction<Row> multi) {

      final MultiResponse mr = new MultiResponse();
      for (Map.Entry<byte[], List<Action<Row>>> entry : multi.actions.entrySet()) {
        for (Action a : entry.getValue()) {
          if (Arrays.equals(FAILS, a.getAction().getRow())) {
            mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), failure);
          } else {
            mr.add(loc.getRegionInfo().getRegionName(), a.getOriginalIndex(), success);
          }
        }
      }

      return new MultiServerCallable<Row>(hConnection, tableName, null, null) {
        @Override
        public MultiResponse withoutRetries() {
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
    Put p = createPut(true, false);
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

  @Test
  public void testMaxTask() throws Exception {
    HConnection hc = createHConnection();
    final AsyncProcess ap = new MyAsyncProcess<Object>(hc, null, conf);

    for (int i = 0; i < 1000; i++) {
      ap.incTaskCounters("dummy");
    }

    final Thread myThread = Thread.currentThread();

    Thread t = new Thread() {
      public void run() {
        Threads.sleep(2000);
        myThread.interrupt();
      }
    };
    t.start();

    try {
      ap.submit(new ArrayList<Row>());
      Assert.fail("We should have been interrupted.");
    } catch (InterruptedIOException expected) {
    }

    final long sleepTime = 2000;

    Thread t2 = new Thread() {
      public void run() {
        Threads.sleep(sleepTime);
        while (ap.taskCounter.get() > 0) {
          ap.decTaskCounters("dummy");
        }
      }
    };
    t2.start();

    long start = System.currentTimeMillis();
    ap.submit(new ArrayList<Row>());
    long end = System.currentTimeMillis();

    //Adds 100 to secure us against approximate timing.
    Assert.assertTrue(start + 100L + sleepTime > end);
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
    public boolean failure(int originalIndex, byte[] region, byte[] row, Throwable t) {
      failureCalled.incrementAndGet();
      return true;
    }

    @Override
    public boolean retriableFailure(int originalIndex, Row row, byte[] region, Throwable exception) {
      // We retry once only.
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

  @Test
  public void testHTablePutSuccess() throws Exception {
    HTable ht = Mockito.mock(HTable.class);
    HConnection hc = createHConnection();
    ht.ap = new MyAsyncProcess<Object>(hc, null, conf);

    Put put = createPut(true, true);

    Assert.assertEquals(0, ht.getWriteBufferSize());
    ht.put(put);
    Assert.assertEquals(0, ht.getWriteBufferSize());
  }

  private void doHTableFailedPut(boolean bufferOn, boolean close) throws Exception {
    HTable ht = new HTable();
    HConnection hc = createHConnection();
    MyCB mcb = new MyCB(); // This allows to have some hints on what's going on.
    ht.ap = new MyAsyncProcess<Object>(hc, mcb, conf);
    ht.setAutoFlush(true, true);
    if (bufferOn){
      ht.setWriteBufferSize(1024L*1024L);
    } else {
      ht.setWriteBufferSize(0L);
    }

    Put put = createPut(true, false);

    Assert.assertEquals(0L, ht.currentWriteBufferSize);
    try {
      ht.put(put);
      if (bufferOn){
        ht.flushCommits();
      }
      Assert.fail();
    }catch (RetriesExhaustedException expected){
    }
    Assert.assertEquals(0L, ht.currentWriteBufferSize);
    Assert.assertEquals(0, mcb.successCalled.get());
    Assert.assertEquals(2, mcb.retriableFailure.get());
    Assert.assertEquals(1, mcb.failureCalled.get());

    if (close){
      // This should not raise any exception, they have been 'received' before by the catch.
      ht.close();
    }
  }

  @Test
  public void testHTableFailedPut() throws Exception {
    doHTableFailedPut(true, false);
    doHTableFailedPut(false, false);
    doHTableFailedPut(true, true);
    doHTableFailedPut(false, true);
  }


    /**
     * @param reg1    if true, creates a put on region 1, region 2 otherwise
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
