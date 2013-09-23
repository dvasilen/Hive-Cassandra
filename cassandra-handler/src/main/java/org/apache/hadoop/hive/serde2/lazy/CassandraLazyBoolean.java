package org.apache.hadoop.hive.serde2.lazy;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql.jdbc.JdbcBoolean;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;

/**
 * CassandraLazyLong parses the object into BooleanWritable value.
 *
 */
public class CassandraLazyBoolean extends LazyBoolean
{
  public CassandraLazyBoolean(LazyBooleanObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    if ( length == 1 ) {
      try {
        ByteBuffer buf = ByteBuffer.wrap(bytes.getData(), start, length);
        data.set(JdbcBoolean.instance.compose(buf));
        isNull = false;
        return;
      } catch (Throwable ie) {
        isNull = true;
      }
    }

    super.init(bytes, start, length);
  }

}

