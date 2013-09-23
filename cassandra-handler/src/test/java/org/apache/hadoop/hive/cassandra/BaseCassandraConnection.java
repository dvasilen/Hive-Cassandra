package org.apache.hadoop.hive.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.contrib.utils.service.CassandraServiceDataCleaner;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class BaseCassandraConnection {

  private static final BaseCassandraConnection instance = new BaseCassandraConnection();

  private BaseCassandraConnection() {

  }

  public static BaseCassandraConnection getInstance() {
    return instance;
  }


  public static CassandraProxyClient client;
  private static EmbeddedCassandraService cassandra;
  public String ksName = "TestKeyspace";
  public String cfName = "TestColumnFamily";

  /**
   * Start the embedded cassandra server if it is not up.
   *
   * @throws java.io.IOException
   * @throws org.apache.thrift.transport.TTransportException
   *
   * @throws org.apache.thrift.TException
   * @throws CassandraException
   */
  public void maybeStartServer() throws IOException, TTransportException, TException, CassandraException {

    File conf = new File("src/test/resources/cassandra.yaml");

    if (!conf.exists()) {
      throw new RuntimeException("Cassandra configuration not found!");
      //System.exit(1);
    }

    System.setProperty("cassandra.config", "file://" + conf.getCanonicalPath());


    if (cassandra == null) {
      CassandraServiceDataCleaner cleaner = new CassandraServiceDataCleaner();
      cleaner.prepare();
      cassandra = new EmbeddedCassandraService();
      cassandra.start();
      client = new CassandraProxyClient(
              "127.0.0.1", 9170, true, true);
      loadSchema();
    }

    Cassandra.Iface proxy = client.getProxyConnection();
    proxy.describe_cluster_name();
  }


  private void loadSchema() {
    for (KSMetaData ksm : schemaDefinition()) {
      for (CFMetaData cfm : ksm.cfMetaData().values()) {

        Schema.instance.load(cfm);
      }
      Schema.instance.setTableDefinition(ksm);
    }
  }

  private Collection<KSMetaData> schemaDefinition() {
    List<KSMetaData> schema = new ArrayList<KSMetaData>();

    Class<? extends AbstractReplicationStrategy> simple = SimpleStrategy.class;

    Map<String, String> opts_rf1 = KSMetaData.optsWithRF(1);

    // initial test keyspace
    schema.add(KSMetaData.testMetadata(
            ksName,
            simple,
            opts_rf1,
            // Add more Column Families as needed
            standardCFMD(ksName, "TestColumnFamily")));


    return schema;
  }

  private static CFMetaData standardCFMD(String ksName, String cfName) {
    return new CFMetaData(ksName, cfName, ColumnFamilyType.Standard, BytesType.instance, null);
  }
}
