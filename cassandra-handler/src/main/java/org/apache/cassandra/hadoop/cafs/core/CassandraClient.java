package org.apache.cassandra.hadoop.cafs.core;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class CassandraClient {


  public static Cassandra.Client createConnection(String host, int port) throws IOException {

    TSocket socket = new TSocket(host, port);
    TTransport trans = new TFramedTransport(socket);
    TProtocol proto = new TBinaryProtocol(trans, true, true);

    try {
      trans.open();
    } catch (TTransportException e) {
      throw new IOException("unable to connect to server", e);
    }

    Cassandra.Client client = new Cassandra.Client(proto);

    return client;
  }

}
