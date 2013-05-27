package org.apache.hadoop.hbase.client;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.exceptions.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class  allows a continuous flow of request.
 * <p>
 * The flow is:<list>
 * <li>buffered, to send the queries by bunch instead of one by one.</li>
 * <li>split by server. If a server is slow or does not respond, the operations on the other
 * servers continue.</li></list>
 * </p>
 * <p>
 * Sending operations is then non blocking, except if:<list>
 * <li>the number of operations in progress is globally too high
 * (setting "hbase.client.max.total.tasks"). In this case, we wait before accepting
 * new operations.</li>
 * <li>A region has reach its maximum number of concurrent task
 * (setting: hbase.client.max.perregion.tasks)
 * in this case, we take all operations on the other regions but the operations on the busy regions
 * are left in the buffer. </li>
 * <li>One of the current operations has failed its maximum number of time. In this case, we try
 * to finish all the operations in progress and trow an exception. to let the user decide.</li>
 * </list>
 * </p>
 * <p>
 * In other words, the process is asynchronous, until: <list>
 * <li>One of the operations has failed.</li>
 * <li>The buffer has been fulled by operations on busy regions</li>  </list>
 * </p>
 *
 * This class is not thread safe externally; only one thread should submit operations.
 */
public class AsyncProcess<Res> {
  private static final Log LOG = LogFactory.getLog(AsyncProcess.class);

  protected final HConnection hConnection;
  protected final byte[] tableName;
  protected final ExecutorService pool;
  protected final AsyncProcessCallback<Res> callback;
  protected final BatchErrors<Row> errors = new BatchErrors<Row>();
  protected final BatchErrors<Row> retriedErrors = new BatchErrors<Row>();
  protected final AtomicBoolean hasError = new AtomicBoolean(false);
  protected final AtomicLong taskCounter = new AtomicLong(0);
  protected final ConcurrentHashMap<String, AtomicInteger> taskCounterPerRegion =
      new ConcurrentHashMap<String, AtomicInteger>();
  protected final int maxTotalConcurrentTasks;
  protected final int maxConcurrentTasksPerRegion;
  protected final long pause;
  protected final int numTries;


  /**
   * This interface allows to keep the interface of the previous synchronous interface, that uses
   * an array of object to return the result.
   *
   * This interface allows the caller to specify the behavior on errors: <list>
   *   <li>If we have not yet reach the maximum number of retries, the user can nevertheless
   *    specify if this specific operation should be retried or not.
   *   </li>
   *   <li>If an operation fails (i.e. is not retried or fails after all retries), the user can
   *    specify is we should mark this AsyncProcess as in error or not.
   *   </li>
   * </list>
   */
  public static interface AsyncProcessCallback<Result> {

    /**
     * Called on success. originalIndex holds the index in the action list.
     */
    void success(int originalIndex, byte[] region, byte[] row, Result result);

    /**
     * called on failure, if we don't retry (i.e. called once per failed operation).
     * @return true if we should store the error and tag this async process as beeing in error.
     *  false if the failure of this operation can be safely ignored.
     */
    boolean failure(int originalIndex, byte[] region, byte[] row, Throwable t);

    /**
     * Called on a failure we plan to retry. This allows the user to stop retrying. Will be
     * called multiple times for a single action if it fails multiple times.
     *
     * @return false if we should retry, true otherwise.
     */
    boolean retriableFailure(int originalIndex, Row row, byte[] region, Throwable exception);
  }

  private static class BatchErrors<R extends Row> {
    private List<Throwable> exceptions = new ArrayList<Throwable>();
    private List<R> actions = new ArrayList<R>();
    private List<String> addresses = new ArrayList<String>();

    public void add(Throwable ex, R row, HRegionLocation location) {
      exceptions.add(ex);
      actions.add(row);
      addresses.add( location != null ? location.getHostnamePort() : "null location");
    }

    private RetriesExhaustedWithDetailsException makeException() {
      return new RetriesExhaustedWithDetailsException(
          new ArrayList<Throwable>(exceptions),
          new ArrayList<Row>(actions), new ArrayList<String>(addresses));
    }

    public void clear(){
      exceptions.clear();
      actions.clear();
      addresses.clear();
    }
  }

  public AsyncProcess(HConnection hc, byte[] tableName, ExecutorService pool,
                      AsyncProcessCallback<Res> callback, Configuration conf) {
    this.hConnection = hc;
    this.tableName = tableName;
    this.pool = pool;
    this.callback = callback;

    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numTries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);

    this.maxTotalConcurrentTasks = conf.getInt("hbase.client.max.total.tasks", 200);

    // With one, we ensure that the ordering of the queries is respected: we don't start
    //  a set of operations on a region before the previous one is done.
    this.maxConcurrentTasksPerRegion = conf.getInt("hbase.client.max.perregion.tasks", 1);
  }

  /**
   * Extract from the rows list what we can submit.
   *
   * @param rows - the rows actually taken will be removed from the list. The rows that
   *             cannot be sent (overloaded region) will be kept in the list.
   */
  public void submit(List<Row> rows) throws InterruptedIOException {
    waitForMaximumTaskNumber(maxTotalConcurrentTasks);

    Map<HRegionLocation, MultiAction<Row>> actionsByServer =
        new HashMap<HRegionLocation, MultiAction<Row>>();
    Map<String, Boolean> regionStatus = new HashMap<String, Boolean>();

    List<Action<Row>> retainedActions = new ArrayList<Action<Row>>(rows.size());

    int posInList = -1;
    Iterator<? extends Row> it = rows.iterator();
    while (it.hasNext()) {
      Row r = it.next();
      HRegionLocation loc = shouldSubmit(r, 1, posInList, false, regionStatus);

      if (loc != null) {
        Action<Row> action = new Action<Row>(r, ++posInList);
        retainedActions.add(action);
        addAction(loc, action, actionsByServer);
        it.remove();
      }
    }

    sendMultiAction(retainedActions, actionsByServer, 1);
  }

  /**
   * Group the actions per region server.
   *
   * @param loc             the destination
   * @param action          the action to add to the multiaction
   * @param actionsByServer the multiaction per servers
   */
  private void addAction(HRegionLocation loc, Action<Row> action, Map<HRegionLocation,
      MultiAction<Row>> actionsByServer) {
    if (loc != null) {
      final byte[] regionName = loc.getRegionInfo().getRegionName();
      MultiAction<Row> multiAction = actionsByServer.get(loc);
      if (multiAction == null) {
        multiAction = new MultiAction<Row>();
        actionsByServer.put(loc, multiAction);
      }

      multiAction.add(regionName, action);
    }
  }

  /**
   * @param row          the row
   * @param numAttempt   the num attempt
   * @param posInList    the position in the list
   * @param force        if we must submit whatever the server load
   * @param regionStatus the
   * @return null if we should not submit, the destination otherwise.
   */
  private HRegionLocation shouldSubmit(Row row, int numAttempt,
                                       int posInList, boolean force,
                                       Map<String, Boolean> regionStatus) {
    HRegionLocation loc = null;
    IOException locationException = null;
    try {
      loc = hConnection.locateRegion(this.tableName, row.getRow());
      if (loc == null) {
        locationException = new IOException("No location found, aborting submit for" +
            " tableName=" + Bytes.toString(tableName));
      }
    } catch (IOException e) {
      locationException = e;
    }
    if (locationException != null) {
      // There are multiple retries in locateRegion already. No need to add new.
      // We can't continue with this row
      manageError(numAttempt, posInList, row, false, locationException, null);
      return null;
    }

    Boolean addIt = force;
    if (!addIt) {
      String regionName = loc.getRegionInfo().getEncodedName();
      addIt = regionStatus.get(regionName);
      if (addIt == null) {
        AtomicInteger ct = taskCounterPerRegion.get(regionName);
        long nbTask = ct == null ? 0 : ct.get();
        addIt = (nbTask < maxConcurrentTasksPerRegion);
        regionStatus.put(regionName, addIt);
        if (LOG.isTraceEnabled()) {
          LOG.debug("Region " + regionName + " has " + nbTask +
              " tasks, max is " + maxConcurrentTasksPerRegion +
              ", " + (addIt ? "" : "NOT ") + "adding task");
        }
      }
    }

    if (!addIt) {
      return null;
    }

    return loc;
  }

  /**
   * Submit immediately the list of rows, whatever the server status. Kept for backward
   * compatibility: it allows to be used with the batch interface that return an array of objects.
   *
   * @param rows the list of rows.
   */
  public void submitAll(List<? extends Row> rows) {
    List<Action<Row>> actions = new ArrayList<Action<Row>>(rows.size());

    // The position will be used by the processBatch to match the object array returned.
    int posInList = -1;
    for (Row r : rows) {
      posInList++;
      Action<Row> action = new Action<Row>(r, posInList);
      actions.add(action);
    }

    submit(actions, actions, 1, true);
  }


  /**
   * Group a list of actions per region servers, and send them. The created MultiActions are
   * added to the inProgress list.
   *
   * @param initialActions the full list of the actions in progress
   * @param currentActions the list of row to submit
   * @param numAttempt     the current numAttempt (first attempt is 1)
   * @param force          true if we submit the rowList without taking into account the server load
   * @throws IOException - if we can't locate a region after multiple retries.
   */
  private void submit(List<Action<Row>> initialActions,
                      List<Action<Row>> currentActions, int numAttempt, boolean force) {
    // group per location => regions server
    final Map<HRegionLocation, MultiAction<Row>> actionsByServer =
        new HashMap<HRegionLocation, MultiAction<Row>>();

    // We have the same policy for a single region per call to submit: we don't want
    //  to send half of the actions because the status changed in the middle. So we keep the
    //  status
    Map<String, Boolean> regionStatus = new HashMap<String, Boolean>();

    for (Action<Row> action : currentActions) {
      HRegionLocation loc = shouldSubmit(
          action.getAction(), 1, action.getOriginalIndex(), force, regionStatus);

      if (loc != null) {
        addAction(loc, action, actionsByServer);
      }
    }

    if (!actionsByServer.isEmpty()) {
      sendMultiAction(initialActions, actionsByServer, numAttempt);
    }
  }

  /**
   * Send a multi action structure to the servers, after a delay depending on the attempt
   * number. Asynchronous.
   *
   * @param initialActions  the list of the actions, flat.
   * @param actionsByServer the actions structured by regions
   * @param numAttempt      the attempt number.
   */
  public void sendMultiAction(List<Action<Row>> initialActions,
                              Map<HRegionLocation, MultiAction<Row>> actionsByServer,
                              int numAttempt) {

    // Send the queries and add them to the inProgress list
    for (Map.Entry<HRegionLocation, MultiAction<Row>> e : actionsByServer.entrySet()) {
      long backoffTime = 0;
      if (numAttempt > 1) {
        backoffTime = ConnectionUtils.getPauseTime(pause, numAttempt - 1);
      }
      Runnable runnable = createDelayedRunnable(initialActions, backoffTime, e.getKey(),
          e.getValue(), numAttempt);
      if (LOG.isTraceEnabled() && numAttempt > 1) {
        StringBuilder sb = new StringBuilder();
        for (Action<Row> action : e.getValue().allActions()) {
          sb.append(Bytes.toStringBinary(action.getAction().getRow())).append(';');
        }
        LOG.trace("Will retry requests to [" + e.getKey().getHostnamePort()
            + "] after delay of [" + backoffTime + "] for rows [" + sb.toString() + "]");
      }

      this.pool.submit(runnable);
    }
  }

  /**
   * Check that we can retry acts accordingly: logs, set the error status, call the callbacks.
   *
   * @param numAttempt    the number of this attempt
   * @param originalIndex the position in the list sent
   * @param row           the row
   * @param canRetry      if false, we won't retry whatever the settings.
   * @param exception     the exception, if any (can be null)
   * @param location      the location, if any (can be null)
   * @return true if the action can be retried, false otherwise.
   */
  private boolean manageError(int numAttempt, int originalIndex, Row row, boolean canRetry,
                              Throwable exception, HRegionLocation location) {
    if (canRetry) {
      if (numAttempt >= numTries ||
          (exception != null && exception instanceof DoNotRetryIOException)) {
        canRetry = false;
      }
    }
    byte[] region = location == null ? null : location.getRegionInfo().getEncodedNameAsBytes();

    if (canRetry && callback != null) {
      canRetry = callback.retriableFailure(originalIndex, row, region, exception);
    }

    if (canRetry) {
      if (LOG.isTraceEnabled()) {
        retriedErrors.add(exception, row, location);
      }
    } else {
      if (callback != null) {
        callback.failure(originalIndex, region, row.getRow(), exception);
      }
      this.hasError.set(true);
      errors.add(exception, row, location);
    }

    return canRetry;
  }

  /**
   * Resubmit all the actions from this multiaction after a failure.
   *
   * @param rsActions  the actions
   * @param location   the destination
   * @param numAttempt the number of attemp so far
   */
  private void resubmitAll(List<Action<Row>> initialActions, MultiAction<Row> rsActions,
                           HRegionLocation location, int numAttempt) {
    List<Action<Row>> toReplay = new ArrayList<Action<Row>>();
    for (List<Action<Row>> actions : rsActions.actions.values()) {
      for (Action<Row> action : actions) {
        // Do not use the exception for updating cache because it might be coming from
        // any of the regions in the MultiAction.
        hConnection.updateCachedLocations(tableName, action.getAction(), null, location);
        if (manageError(numAttempt, action.getOriginalIndex(), action.getAction(),
            true, null, null)) {
          toReplay.add(action);
        }
      }
    }
    submit(initialActions, toReplay, numAttempt, true);
  }

  /**
   * Called when we receive the result of a server query.
   *
   * @param initialActions - the whole action list
   * @param rsActions      - the actions for this location
   * @param location       - the location
   * @param responses      - the response, if any
   * @param numAttempt     - the attempt
   * @throws InterruptedException
   * @throws IOException
   */
  private void receiveMultiAction(List<Action<Row>> initialActions,
                                  MultiAction<Row> rsActions, HRegionLocation location,
                                  MultiResponse responses, int numAttempt) {

    if (responses == null) {
      LOG.info("Attempt #" + numAttempt + " failed for all operations on server " +
          location.getServerName() + " , trying to resubmit.");
      resubmitAll(initialActions, rsActions, location, numAttempt + 1);
      return;
    }

    // Success or partial success
    // Analyze detailed results. We can still have individual failures to be redo.
    // two specific exceptions are managed:
    //  - DoNotRetryIOException: we continue to retry for other actions
    //  - RegionMovedException: we update the cache with the new region location

    List<Action<Row>> toReplay = new ArrayList<Action<Row>>();

    int failureCount = 0;
    for (Map.Entry<byte[], List<Pair<Integer, Object>>> resultsForRS :
        responses.getResults().entrySet()) {

      for (Pair<Integer, Object> regionResult : resultsForRS.getValue()) {
        Object result = regionResult.getSecond();

        // Failure: retry if it's make sense else update the errors lists
        if (result == null || result instanceof Throwable) {
          failureCount++;
          Action<Row> correspondingAction = initialActions.get(regionResult.getFirst());
          Row row = correspondingAction.getAction();
          hConnection.updateCachedLocations(this.tableName, row, result, location);

          if (manageError(numAttempt, correspondingAction.getOriginalIndex(), row, true,
              (Throwable) result, location)) {
            toReplay.add(correspondingAction);
          }
        } else // success
          if (callback != null) {
            Action<Row> correspondingAction = initialActions.get(regionResult.getFirst());
            Row row = correspondingAction.getAction();
            //noinspection unchecked
            this.callback.success(correspondingAction.getOriginalIndex(),
                resultsForRS.getKey(), row.getRow(), (Res) result);
          }
      }
    }

    if (!toReplay.isEmpty()) {
      LOG.info("Attempt #" + numAttempt + " failed for " + failureCount +
          " operations on server " + location.getServerName() + ", resubmitting " +
          toReplay.size() + ", tableName=" + Bytes.toString(tableName));
      submit(initialActions, toReplay, numAttempt + 1, true);
    } else if (failureCount != 0) {
      LOG.warn("Attempt #" + numAttempt + " failed for " + failureCount +
          "operations on server " + location.getServerName() + " NOT resubmitting." +
          ", tableName=" + Bytes.toString(tableName));
    }
  }


  /**
   * Wait until the async does not have more than max tasks in progress.
   */
  private void waitForMaximumTaskNumber(int max) throws InterruptedIOException {
    long lastLog = 0;
    while (this.taskCounter.get() > max) {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      if (now > lastLog + 5000) {
        lastLog = now;
        LOG.info(Bytes.toString(tableName) +
            ": Waiting for number of tasks to be equals or less than " + max +
            ", currently it's " + this.taskCounter.get());
      }
      try {
        synchronized (this.taskCounter) {
          this.taskCounter.wait(200);
        }
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }
  }

  /**
   * Wait until all tasks are executed, successfully or not.
   */
  public void waitUntilDone() throws InterruptedIOException {
    waitForMaximumTaskNumber(0);
  }

  /**
   *
   * @return
   */
  public boolean hasError() {
    return hasError.get();
  }

  public List<? extends Row> getFailedOperations() {
    return errors.actions;
  }

  /**
   * Clean the errors stacks. Should be called only when there are no actions in progress.
   */
  public void clearErrors() {
    errors.clear();
    retriedErrors.clear();
    hasError.set(false);
  }

  public RetriesExhaustedWithDetailsException getErrors() {
    return errors.makeException();
  }

  /**
   * incrementer the tasks counters for a given region. MT safe.
   */
  protected void incTaskCounters(String encodedRegionName) {
    taskCounter.incrementAndGet();

    AtomicInteger counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    if (counterPerServer == null) {
      taskCounterPerRegion.putIfAbsent(encodedRegionName, new AtomicInteger());
      counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    }
    counterPerServer.incrementAndGet();
  }

  /**
   * Decrements the counters for a given region
   */
  protected void decTaskCounters(String encodedRegionName) {
    taskCounter.decrementAndGet();

    AtomicInteger counterPerServer = taskCounterPerRegion.get(encodedRegionName);
    counterPerServer.decrementAndGet();

    synchronized (taskCounter) {
      taskCounter.notifyAll();
    }
  }


  /**
   * Create a specific Runnable that will wait a given amount of ms before executing the
   * call to the region servers.
   *
   * @param initialActions - the original list of actions
   * @param delay          - how long to wait
   * @param loc            - location
   * @param multi          - the multi action to execute
   * @param numAttempt     - the number of attempt
   * @return the Runnable
   */
  private Runnable createDelayedRunnable(final List<Action<Row>> initialActions,
                                         final long delay, final HRegionLocation loc,
                                         final MultiAction<Row> multi, final int numAttempt) {

    final Callable<MultiResponse> delegate = createCallable(hConnection, loc, multi, tableName);
    final String regionName = loc.getRegionInfo().getEncodedName();
    incTaskCounters(regionName);

    return new Runnable() {
      private final long creationTime = EnvironmentEdgeManager.currentTimeMillis();

      @Override
      public void run() {
        try {
          long waitingTime = delay + creationTime - EnvironmentEdgeManager.currentTimeMillis();
          if (waitingTime > 0) {
            try {
              Thread.sleep(waitingTime);
            } catch (InterruptedException e) {
              LOG.warn("We've been interrupted while waiting to send" +
                  " for regionName=" + regionName);
              resubmitAll(initialActions, multi, loc, numAttempt + 1);
              Thread.interrupted();
              return;
            }
          }
          MultiResponse res;
          try {
            res = delegate.call();
          } catch (Exception e) {
            LOG.warn("The call to the RS failed, we don't know where we stand. regionName="
                + regionName, e);
            resubmitAll(initialActions, multi, loc, numAttempt + 1);
            return;
          }
          receiveMultiAction(initialActions, multi, loc, res, numAttempt);
        } finally {
          decTaskCounters(regionName);
        }
      }
    };
  }

  protected <R> Callable<MultiResponse> createCallable(final HConnection connection,
                                                     final HRegionLocation loc,
                                                     final MultiAction<R> multi,
                                                     final byte[] tableName) {
    return new Callable<MultiResponse>() {
      @Override
      public MultiResponse call() throws Exception {
        ServerCallable<MultiResponse> callable =
            new MultiServerCallable<R>(connection, tableName, loc, multi);
        return callable.withoutRetries();
      }
    };
  }
}
