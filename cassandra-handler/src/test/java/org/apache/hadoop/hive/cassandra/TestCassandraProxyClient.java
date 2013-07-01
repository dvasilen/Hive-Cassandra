package org.apache.hadoop.hive.cassandra;

import java.util.List;

import junit.framework.TestCase;
import junit.framework.TestResult;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TestCassandraProxyClient extends TestCase {

  private BaseCassandraConnection bcc = BaseCassandraConnection.getInstance();

  /**
   * Make sure that when the server is down, proxy client will only try a certain amount of times and fails the request.
   * Please make sure to run this as the first test.
   *
   * @throws Exception
   */
  public void testServerDown() throws Exception {

    try {
      CassandraProxyClient client = new CassandraProxyClient(
              "127.0.0.1", 9170, true, true);
      client.getProxyConnection().describe_keyspaces();
      fail("Fail this test.");
    } catch (CassandraException e) {
      //As expected.
    }
  }


  public void testInsertionQuery() throws Exception {

    bcc.maybeStartServer();
    List<KsDef> keyspaces = bcc.client.getProxyConnection().describe_keyspaces();
    assertTrue(keyspaces.size() > 1);


    for (KsDef thisKs : keyspaces) {
      if (!thisKs.getName().equals("system")) {
        bcc.ksName = thisKs.getName();
        break;
      }
    }

    CfDef columnFamily = new CfDef(bcc.ksName, "TestCassandra");
    bcc.client.getProxyConnection().set_keyspace(bcc.ksName);
    bcc.client.getProxyConnection().system_add_column_family(columnFamily);

    //add some data
    Column column = new Column()
            .setName(ByteBufferUtil.bytes("name"))
            .setValue(ByteBufferUtil.bytes("value"))
            .setTimestamp(System.currentTimeMillis());

    bcc.client.getProxyConnection().insert(ByteBufferUtil.bytes("key1"), new ColumnParent(bcc.cfName), column, ConsistencyLevel.ALL);

    //query for the data
    ColumnPath path = new ColumnPath();
    path.setColumn_family(bcc.cfName);
    path.setColumn(ByteBufferUtil.bytes("name"));
    ColumnOrSuperColumn result = bcc.client.getProxyConnection().get(ByteBufferUtil.bytes("key1"), path, ConsistencyLevel.ALL);
    assertNotNull(result);
    assertEquals("name", new String(result.getColumn().getName()));
    assertEquals("value", new String(result.getColumn().getValue()));

  }
}
