package org.apache.hadoop.hive.cassandra.ql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;

public class UDFCompositeToByteArray extends GenericUDF{

  private ObjectInspector [] argumentOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
          throws UDFArgumentException {
    this.argumentOI = arguments;
    if (arguments.length != 1 ) {
      throw new UDFArgumentLengthException("takes 1 argument "+arguments.length);
    }
    return ObjectInspectorFactory.getStandardListObjectInspector
            (PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    BytesWritable columnAsBinary = ((BinaryObjectInspector) argumentOI[0])
            .getPrimitiveWritableObject(arguments[0].get());
    return evalInternal(columnAsBinary);
  }

  public List<ByteArrayRef> evalInternal ( BytesWritable columnAsBinary ){
    byte [] rk = new byte [columnAsBinary.getLength()];
    for (int i =0;i<rk.length;i++){
      rk[i]=columnAsBinary.getBytes()[i];
    }
    List<ByteArrayRef> res = new ArrayList<ByteArrayRef>();
    for (byte[] bytes : CompositeTool.readComposite(rk)){
      ByteArrayRef ref = new ByteArrayRef();
      ref.setData(bytes);
      res.add(ref);
    }
    return res;
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "Cassandra rocks";
  }

}
