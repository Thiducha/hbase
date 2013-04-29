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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.protobuf.generated.WAL.CompactionDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.Writable;

/**
 * WALEdit: Used in HBase's transaction log (WAL) to represent
 * the collection of edits (KeyValue objects) corresponding to a
 * single transaction. The class implements "Writable" interface
 * for serializing/deserializing a set of KeyValue items.
 *
 * Previously, if a transaction contains 3 edits to c1, c2, c3 for a row R,
 * the HLog would have three log entries as follows:
 *
 *    <logseq1-for-edit1>:<KeyValue-for-edit-c1>
 *    <logseq2-for-edit2>:<KeyValue-for-edit-c2>
 *    <logseq3-for-edit3>:<KeyValue-for-edit-c3>
 *
 * This presents problems because row level atomicity of transactions
 * was not guaranteed. If we crash after few of the above appends make
 * it, then recovery will restore a partial transaction.
 *
 * In the new world, all the edits for a given transaction are written
 * out as a single record, for example:
 *
 *   <logseq#-for-entire-txn>:<WALEdit-for-entire-txn>
 *
 * where, the WALEdit is serialized as:
 *   <-1, # of edits, <KeyValue>, <KeyValue>, ... >
 * For example:
 *   <-1, 3, <Keyvalue-for-edit-c1>, <KeyValue-for-edit-c2>, <KeyValue-for-edit-c3>>
 *
 * The -1 marker is just a special way of being backward compatible with
 * an old HLog which would have contained a single <KeyValue>.
 *
 * The deserializer for WALEdit backward compatibly detects if the record
 * is an old style KeyValue or the new style WALEdit.
 *
 */
@InterfaceAudience.Private
public class WALEdit implements Writable, HeapSize {
  // TODO: Make it so user cannot make a cf w/ this name.  Make the illegal cf names.  Ditto for row.
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte [] METAROW = Bytes.toBytes("METAROW");
  static final byte[] COMPLETE_CACHE_FLUSH = Bytes.toBytes("HBASE::CACHEFLUSH");
  static final byte[] COMPACTION = Bytes.toBytes("HBASE::COMPACTION");

  private final int VERSION_2 = -1;

  private final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
  private NavigableMap<byte[], Integer> scopes;

  private CompressionContext compressionContext;

  public WALEdit() {
  }

  /**
   * @param f
   * @return True is <code>f</code> is {@link #METAFAMILY}
   */
  public static boolean isMetaEditFamily(final byte [] f) {
    return Bytes.equals(METAFAMILY, f);
  }

  public void setCompressionContext(final CompressionContext compressionContext) {
    this.compressionContext = compressionContext;
  }

  public WALEdit add(KeyValue kv) {
    this.kvs.add(kv);
    return this;
  }

  public boolean isEmpty() {
    return kvs.isEmpty();
  }

  public int size() {
    return kvs.size();
  }

  public List<KeyValue> getKeyValues() {
    return kvs;
  }

  public NavigableMap<byte[], Integer> getScopes() {
    return scopes;
  }


  public void setScopes (NavigableMap<byte[], Integer> scopes) {
    // We currently process the map outside of WALEdit,
    // TODO revisit when replication is part of core
    this.scopes = scopes;
  }

  public void readFields(DataInput in) throws IOException {
    kvs.clear();
    if (scopes != null) {
      scopes.clear();
    }
    int versionOrLength = in.readInt();
    // TODO: Change version when we protobuf.  Also, change way we serialize KV!  Pb it too.
    if (versionOrLength == VERSION_2) {
      // this is new style HLog entry containing multiple KeyValues.
      int numEdits = in.readInt();
      for (int idx = 0; idx < numEdits; idx++) {
        if (compressionContext != null) {
          this.add(KeyValueCompression.readKV(in, compressionContext));
        } else {
          this.add(KeyValue.create(in));
    	  }
      }
      int numFamilies = in.readInt();
      if (numFamilies > 0) {
        if (scopes == null) {
          scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
        }
        for (int i = 0; i < numFamilies; i++) {
          byte[] fam = Bytes.readByteArray(in);
          int scope = in.readInt();
          scopes.put(fam, scope);
        }
      }
    } else {
      // this is an old style HLog entry. The int that we just
      // read is actually the length of a single KeyValue
      this.add(KeyValue.create(versionOrLength, in));
    }

  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION_2);
    out.writeInt(kvs.size());
    // We interleave the two lists for code simplicity
    for (KeyValue kv : kvs) {
      if (compressionContext != null) {
        KeyValueCompression.writeKV(out, kv, compressionContext);
      } else{
        KeyValue.write(kv, out);
      }
    }
    if (scopes == null) {
      out.writeInt(0);
    } else {
      out.writeInt(scopes.size());
      for (byte[] key : scopes.keySet()) {
        Bytes.writeByteArray(out, key);
        out.writeInt(scopes.get(key));
      }
    }
  }

  public long heapSize() {
    long ret = 0;
    for (KeyValue kv : kvs) {
      ret += kv.heapSize();
    }
    if (scopes != null) {
      ret += ClassSize.TREEMAP;
      ret += ClassSize.align(scopes.size() * ClassSize.MAP_ENTRY);
      // TODO this isn't quite right, need help here
    }
    return ret;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("[#edits: " + kvs.size() + " = <");
    for (KeyValue kv : kvs) {
      sb.append(kv.toString());
      sb.append("; ");
    }
    if (scopes != null) {
      sb.append(" scopes: " + scopes.toString());
    }
    sb.append(">]");
    return sb.toString();
  }

  /**
   * Create a compacion WALEdit
   * @param c
   * @return A WALEdit that has <code>c</code> serialized as its value
   */
  public static WALEdit createCompaction(final CompactionDescriptor c) {
    byte [] pbbytes = c.toByteArray();
    KeyValue kv = new KeyValue(METAROW, METAFAMILY, COMPACTION, System.currentTimeMillis(), pbbytes);
    return new WALEdit().add(kv); //replication scope null so that this won't be replicated
  }

  /**
   * Deserialized and returns a CompactionDescriptor is the KeyValue contains one.
   * @param kv the key value
   * @return deserialized CompactionDescriptor or null.
   */
  public static CompactionDescriptor getCompaction(KeyValue kv) throws IOException {
    if (kv.matchingRow(METAROW) && kv.matchingColumn(METAFAMILY, COMPACTION)) {
      return CompactionDescriptor.parseFrom(kv.getValue());
    }
    return null;
  }
}