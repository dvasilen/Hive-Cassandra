package org.apache.hadoop.hive.cassandra;

import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.DateType;
import org.apache.cassandra.thrift.ColumnDef;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class CassandraPushdownPredicateTest {


  @Test
  public void roundTripIndexedColumnDefinitions()
  {
    Set<ColumnDef> input = new HashSet<ColumnDef>();
    ColumnDef c0 = new ColumnDef();
    c0.setName("col0".getBytes());
    c0.setValidation_class(DateType.class.getName());
    ColumnDef c1 = new ColumnDef();
    c1.setName("c1".getBytes());
    c1.setValidation_class(BytesType.class.getName());
    input.add(c0);
    input.add(c1);
    Set<ColumnDef> output = CassandraPushdownPredicate.deserializeIndexedColumns(
                              CassandraPushdownPredicate.serializeIndexedColumns(input));
    assertTrue(Iterables.elementsEqual(input, output));
  }
}
