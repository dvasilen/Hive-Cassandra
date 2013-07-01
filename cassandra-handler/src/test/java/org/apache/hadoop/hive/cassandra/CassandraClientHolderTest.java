package org.apache.hadoop.hive.cassandra;

import junit.framework.TestCase;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class CassandraClientHolderTest extends TestCase {
  private final Logger log = LoggerFactory.getLogger(CassandraClientHolderTest.class);
  private BaseCassandraConnection bcc = BaseCassandraConnection.getInstance();

  public void testBuildClientHolder() throws Exception {
    bcc.maybeStartServer();
    TSocket socket = new TSocket("127.0.0.1", 9170);
    TTransport trans = new TFramedTransport(socket);

    CassandraClientHolder clientHolder = new CassandraClientHolder(trans, bcc.ksName);

    assertEquals("Test Cluster", clientHolder.getClient().describe_cluster_name());

  }

  public void testIsOpen() {
    try {
      bcc.maybeStartServer();
      TSocket socket = new TSocket("127.0.0.1", 9170);
      TTransport trans = new TFramedTransport(socket);
      CassandraClientHolder clientHolder = new CassandraClientHolder(trans, bcc.ksName);
      assertTrue(clientHolder.isOpen());
      clientHolder.close();
      assertFalse(clientHolder.isOpen());
    } catch (Exception ex) {
      log.error("", ex);
      fail();
    }
  }

  public void testSetKeyspace() throws Exception {
    bcc.maybeStartServer();
    TSocket socket = new TSocket("127.0.0.1", 9170);
    TTransport trans = new TFramedTransport(socket);
    CassandraClientHolder clientHolder = new CassandraClientHolder(trans);
    assertNull(clientHolder.getKeyspace());
    clientHolder.setKeyspace(bcc.ksName);
    Cassandra.Client client = clientHolder.getClient();
    assertNotNull(client);
    assertEquals(bcc.ksName, clientHolder.getKeyspace());
  }
}
