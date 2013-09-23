package org.apache.hadoop.hive.cassandra.ql.udf;

import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BytesWritable;

public class UDFCompositeTest extends UDF {

  public String evaluate(final BytesWritable s) {
    if (s == null) {
      return null;
    }
    // byte buffer
    ByteBuffer bb = ByteBuffer.wrap(s.getBytes());

    // first part of composite
    int colASize = (bb.get() & 0xFF) << (7 + 1);
    colASize = (bb.get() & 0xFF);
    byte[] colA = new byte[colASize];
    bb.get(colA);
    ByteBuffer bf = ByteBuffer.wrap(colA);

    bb.get();
    // second part of composite
    int colBSize = (bb.get() & 0xFF) << (7 + 1);
    colBSize = (bb.get() & 0xFF);
    byte[] colB = new byte[colBSize];
    bb.get(colB);

    // return concatenated string
    return new String(bf.getLong() + "_" + new String(colB));
  }
}

