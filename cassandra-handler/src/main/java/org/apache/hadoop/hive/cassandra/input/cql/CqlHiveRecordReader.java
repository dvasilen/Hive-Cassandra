package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cassandra.serde.cql.AbstractCqlSerDe;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

public class CqlHiveRecordReader extends RecordReader<MapWritable, MapWritable>
        implements org.apache.hadoop.mapred.RecordReader<MapWritable, MapWritable> {

  static final Log LOG = LogFactory.getLog(CqlHiveRecordReader.class);

  //private final boolean isTransposed;
  private final CqlPagingRecordReader cfrr;
  private Iterator<Map.Entry<String, ByteBuffer>> columnIterator = null;
  private Map.Entry<String, ByteBuffer> currentEntry;
  //private Iterator<IColumn> subColumnIterator = null;
  private MapWritable currentKey = null;
  private final MapWritable currentValue = new MapWritable();

  public static final BytesWritable keyColumn = new BytesWritable(AbstractCqlSerDe.CASSANDRA_KEY_COLUMN.getBytes());
  public static final BytesWritable columnColumn = new BytesWritable(AbstractCqlSerDe.CASSANDRA_COLUMN_COLUMN.getBytes());
  public static final BytesWritable subColumnColumn = new BytesWritable(AbstractCqlSerDe.CASSANDRA_SUBCOLUMN_COLUMN.getBytes());
  public static final BytesWritable valueColumn = new BytesWritable(AbstractCqlSerDe.CASSANDRA_VALUE_COLUMN.getBytes());


  public CqlHiveRecordReader(CqlPagingRecordReader cprr) { //, boolean isTransposed) {
    this.cfrr = cprr;
    //this.isTransposed = isTransposed;
  }

  @Override
  public void close() throws IOException {
    cfrr.close();
  }

  @Override
  public MapWritable createKey() {
    return new MapWritable();
  }

  @Override
  public MapWritable createValue() {
    return new MapWritable();
  }

  @Override
  public long getPos() throws IOException {
    return cfrr.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return cfrr.getProgress();
  }

  public static int callCount = 0;

  @Override
  public boolean next(MapWritable key, MapWritable value) throws IOException {
    if (!nextKeyValue()) {
      return false;
    }

    key.clear();
    key.putAll(getCurrentKey());

    value.clear();
    value.putAll(getCurrentValue());

    return true;
  }

  @Override
  public MapWritable getCurrentKey() {
    return currentKey;
  }

  @Override
  public MapWritable getCurrentValue() {
    return currentValue;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    cfrr.initialize(split, context);
  }

  private BytesWritable convertByteBuffer(ByteBuffer val) {
    return new BytesWritable(ByteBufferUtil.getArray(val));
  }

  @Override
  public boolean nextKeyValue() throws IOException {

    boolean next = false;

    // In the case that we are transposing we create a fixed set of columns
    // per cassandra column
    next = cfrr.nextKeyValue();

    currentValue.clear();

    if (next) {
      currentKey = mapToMapWritable(cfrr.getCurrentKey());

      // rowKey
      currentValue.putAll(currentKey);
      currentValue.putAll(mapToMapWritable(cfrr.getCurrentValue()));
      //populateMap(cfrr.getCurrentValue(), currentValue);
    }

    return next;
  }

  private MapWritable mapToMapWritable(Map<String, ByteBuffer> map) {
    MapWritable mw = new MapWritable();
    for (Map.Entry<String, ByteBuffer> e : map.entrySet()) {
      mw.put(new Text(e.getKey()), convertByteBuffer(e.getValue()));
    }
    return mw;
  }

/*  private void populateMap(Map<Map<String, ByteBuffer>, Map<String, ByteBuffer>> cvalue, MapWritable value) {
    for (Map.Entry<Map<String, ByteBuffer>, Map<String, ByteBuffer>> e : cvalue.entrySet()) {
      Map<String, ByteBuffer> k = e.getKey();
      Map<String, ByteBuffer> v = e.getValue();


      if (!v.isLive()) {
        continue;
      }


      BytesWritable newKey = k);
      BytesWritable newValue = convertByteBuffer(v.value());

      value.put(newKey, newValue);
    }
  } */
}
