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

package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.NavigableSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.wal.HLogMetrics.Metric;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.classification.InterfaceAudience;


@InterfaceAudience.Private
public interface HLog {
  public static final Log LOG = LogFactory.getLog(HLog.class);

  public static final byte[] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte[] METAROW = Bytes.toBytes("METAROW");

  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;

  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  static final String RECOVERED_EDITS_DIR = "recovered.edits";
  static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";

  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;

    void close() throws IOException;

    Entry next() throws IOException;

    Entry next(Entry reuse) throws IOException;

    void seek(long pos) throws IOException;

    long getPosition() throws IOException;
  }

  public interface Writer {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;

    void close() throws IOException;

    void sync() throws IOException;

    void append(Entry entry) throws IOException;

    long getLength() throws IOException;
  }

  /**
   * Utility class that lets us keep track of the edit with it's key Only used
   * when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * 
     * @param edit
     *          log's edit
     * @param key
     *          log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }

    /**
     * Gets the edit
     * 
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }

    /**
     * Gets the key
     * 
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    /**
     * Set compression context for this entry.
     * 
     * @param compressionContext
     *          Compression context
     */
    public void setCompressionContext(CompressionContext compressionContext) {
      edit.setCompressionContext(compressionContext);
      key.setCompressionContext(compressionContext);
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /*
   * registers WALActionsListener
   * 
   * @param listener
   */
  public void registerWALActionsListener(final WALActionsListener listener);

  /*
   * unregisters WALActionsListener
   * 
   * @param listener
   */
  public boolean unregisterWALActionsListener(final WALActionsListener listener);

  /**
   * @return Current state of the monotonically increasing file id.
   */
  public long getFilenum();

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   * 
   * @param newvalue
   *          We'll set log edit/sequence number to this value if it is greater
   *          than the current value.
   */
  public void setSequenceNumber(final long newvalue);

  /**
   * @return log sequence number
   */
  public long getSequenceNumber();

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * 
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   * 
   * <p>
   * Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   * 
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte[][] rollWriter() throws FailedLogCloseException, IOException;

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * 
   * Because a log cannot be rolled during a cache flush, and a cache flush
   * spans two method calls, a special lock needs to be obtained so that a cache
   * flush cannot start when the log is being rolled and the log cannot be
   * rolled during a cache flush.
   * 
   * <p>
   * Note that this method cannot be synchronized because it is possible that
   * startCacheFlush runs, obtaining the cacheFlushLock, then this method could
   * start which would obtain the lock on this but block on obtaining the
   * cacheFlushLock and then completeCacheFlush could be called which would wait
   * for the lock on this and consequently never release the cacheFlushLock
   * 
   * @param force
   *          If true, force creation of a new writer even if no entries have
   *          been written to the current writer
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte[][] rollWriter(boolean force) throws FailedLogCloseException,
      IOException;

  /**
   * Shut down the log.
   * 
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Shut down the log and delete the log directory
   * 
   * @throws IOException
   */
  public void closeAndDelete() throws IOException;

  /**
   * Append an entry to the log.
   * 
   * @param regionInfo
   * @param logEdit
   * @param logKey
   * @param doSync
   *          shall we sync after writing the transaction
   * @return The txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo regionInfo, HLogKey logKey, WALEdit logEdit,
      HTableDescriptor htd, boolean doSync) throws IOException;

  /**
   * Only used in tests.
   * 
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @param htd
   * @throws IOException
   */
  public void append(HRegionInfo info, byte[] tableName, WALEdit edits,
      final long now, HTableDescriptor htd) throws IOException;

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is not flushed after
   * this transaction is written to the log.
   * 
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId
   *          The originating clusterId for this edit (for replication)
   * @param now
   * @return txid of this transaction
   * @throws IOException
   */
  public long appendNoSync(HRegionInfo info, byte[] tableName, WALEdit edits,
      UUID clusterId, final long now, HTableDescriptor htd) throws IOException;

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is flushed after this
   * transaction is written to the log.
   * 
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId
   *          The originating clusterId for this edit (for replication)
   * @param now
   * @param htd
   * @return txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo info, byte[] tableName, WALEdit edits,
      UUID clusterId, final long now, HTableDescriptor htd) throws IOException;

  public void hsync() throws IOException;

  public void hflush() throws IOException;

  public void sync() throws IOException;

  public void sync(long txid) throws IOException;

  /**
   * Obtain a log sequence number.
   */
  public long obtainSeqNum();

  /**
   * By acquiring a log sequence ID, we can allow log messages to continue while
   * we flush the cache.
   * 
   * Acquire a lock so that we do not roll the log between the start and
   * completion of a cache-flush. Otherwise the log-seq-id for the flush will
   * not appear in the correct logfile.
   * 
   * Ensuring that flushes and log-rolls don't happen concurrently also allows
   * us to temporarily put a log-seq-number in lastSeqWritten against the region
   * being flushed that might not be the earliest in-memory log-seq-number for
   * that region. By the time the flush is completed or aborted and before the
   * cacheFlushLock is released it is ensured that lastSeqWritten again has the
   * oldest in-memory edit's lsn for the region that was being flushed.
   * 
   * In this method, by removing the entry in lastSeqWritten for the region
   * being flushed we ensure that the next edit inserted in this region will be
   * correctly recorded in
   * {@link #append(HRegionInfo, byte[], WALEdit, long, HTableDescriptor)} The
   * lsn of the earliest in-memory lsn - which is now in the memstore snapshot -
   * is saved temporarily in the lastSeqWritten map while the flush is active.
   * 
   * @return sequence ID to pass
   *         {@link #completeCacheFlush(byte[], byte[], long, boolean)} (byte[],
   *         byte[], long)}
   * @see #completeCacheFlush(byte[], byte[], long, boolean)
   * @see #abortCacheFlush(byte[])
   */
  public long startCacheFlush(final byte[] encodedRegionName);

  /**
   * Complete the cache flush
   * 
   * Protected by cacheFlushLock
   * 
   * @param encodedRegionName
   * @param tableName
   * @param logSeqId
   * @throws IOException
   */
  public void completeCacheFlush(final byte[] encodedRegionName,
      final byte[] tableName, final long logSeqId, final boolean isMetaRegion)
      throws IOException;

  /**
   * Abort a cache flush. Call if the flush fails. Note that the only recovery
   * for an aborted flush currently is a restart of the regionserver so the
   * snapshot content dropped by the failure gets restored to the memstore.
   */
  public void abortCacheFlush(byte[] encodedRegionName);

  /**
   * @return Coprocessor host.
   */
  public WALCoprocessorHost getCoprocessorHost();

  /**
   * Get LowReplication-Roller status
   * 
   * @return lowReplicationRollEnabled
   */
  public boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
     return (Class<? extends HLogKey>)
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    /**
     * Set compression context for this entry.
     * @param compressionContext Compression context
     */
    public void setCompressionContext(CompressionContext compressionContext) {
      edit.setCompressionContext(compressionContext);
      key.setCompressionContext(compressionContext);
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the relative HLog directory name, e.g. <code>.logs/1.example.org,60030,12345</code>
   * if <code>serverName</code> passed is <code>1.example.org,60030,12345</code>
   */
  public static String getHLogDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }


  /**
   * @param path - the path to analyze. Expected format, if it's in hlog directory:
   *  / [base directory for hbase] / hbase / .logs / ServerName / logfile
   *             or
   *  / [base directory for hbase] / hbase / .logs / ServerName-splitting / logfile
   * @return null if it's not a log file. Returns the ServerName of the region server that created
   *  this log file otherwise.
   */
  public static ServerName getServerNameFromHLogDirectoryName(Configuration conf, String path)
      throws IOException {
    if (path == null || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir == null || rootDir.isEmpty()) {
      throw new IllegalArgumentException(HConstants.HBASE_DIR + " key not found in conf.");
    }

    final StringBuilder startPathSB = new StringBuilder(rootDir);
    if (!rootDir.endsWith("/")) startPathSB.append('/');
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/")) startPathSB.append('/');
    final String startPath = startPathSB.toString();

    String fullPath;
    try {
      fullPath = FileSystem.get(conf).makeQualified(new Path(path)).toString();
    } catch (IllegalArgumentException e) {
      LOG.info("Call to makeQualified failed on " + path + " " + e.getMessage());
      return null;
    }

    if (!fullPath.startsWith(startPath)) {
      return null;
    }

    final String serverNameAndFile = fullPath.substring(startPath.length());

    final int minLength = "a,0,0".length();
    if (serverNameAndFile.indexOf('/') < minLength) {
      // Either it's a file (not a directory) or it's not a ServerName format
      return null;
    }

    String serverName = serverNameAndFile.substring(0, serverNameAndFile.indexOf('/'));
    final String splitting = "-splitting";
    if (serverName.endsWith(splitting) && serverName.lastIndexOf('-') > minLength) {
      serverName = serverName.substring(0, serverName.lastIndexOf('-'));
    }

    if (!ServerName.isFullServerName(serverName)) {
      return null;
    }

    return ServerName.parseServerName(serverName);
  }

  /**
   * Get the directory we are making logs in.
   * 
   * @return dir
   */
  protected Path getDir() {
    return dir;
  }
  
  /**
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an HLog, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir)) return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix.  See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splithlog thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null) return filesSorted;
    for (FileStatus status: files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   * @param fs
   * @param edits Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs,
      final Path edits)
  throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "." +
      System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param regiondir This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   * <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, RECOVERED_EDITS_DIR);
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void usage() {
    System.err.println("Usage: HLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: HLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: HLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  private static void split(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    final Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException(p + " is not a directory");
    }

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(
        conf, baseDir, p, oldLogDir, fs);
    logSplitter.splitLog();
  }

  /**
   * @return Coprocessor host.
   */
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** Provide access to currently deferred sequence num for tests */
  boolean hasDeferredEntries() {
    return lastDeferredTxid > syncedTillHere;
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the HLogPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      HLogPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          conf.set("fs.default.name", args[i]);
          conf.set("fs.defaultFS", args[i]);
          Path logPath = new Path(args[i]);
          split(conf, logPath);
        } catch (Throwable t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }
>>>>>>> 7d7f481addfec9692e39a468151cdbd3666545a7
}
