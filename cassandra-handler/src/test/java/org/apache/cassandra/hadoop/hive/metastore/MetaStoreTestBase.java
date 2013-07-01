package org.apache.cassandra.hadoop.hive.metastore;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.BaseCassandraConnection;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.thrift.TException;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

//import org.apache.cassandra.CleanupHelper;
//import org.apache.cassandra.EmbeddedServer;


public abstract class MetaStoreTestBase extends CleanupHelper {

  @BeforeClass
  public static void setup() throws TException, IOException, InterruptedException, ConfigurationException, CassandraException {
    BaseCassandraConnection.getInstance().maybeStartServer();
  }

  protected Configuration buildConfiguration() {
    Configuration conf = new Configuration();
    conf.set(CassandraClientHolder.CONF_PARAM_HOST, "localhost");
    conf.setInt(CassandraClientHolder.CONF_PARAM_PORT, DatabaseDescriptor.getRpcPort());
    conf.setBoolean(CassandraClientHolder.CONF_PARAM_FRAMED, true);
    conf.set(CassandraClientHolder.CONF_PARAM_CONNECTION_STRATEGY, "STICKY");
    conf.set("hive.metastore.warehouse.dir", "cfs:///user/hive/warehouse");
    return conf;
  }

  /**
   * Builds out a KsDef, does not persist.
   *
   * @param ksName
   * @return
   * @throws Exception
   */
  protected KsDef setupOtherKeyspace(Configuration configuration, String ksName, boolean addMetaData) throws Exception {
    return setupOtherKeyspace(configuration, ksName, "UTF8Type", "UTF8Type", addMetaData);
  }

  protected KsDef setupOtherKeyspace(Configuration configuration, String ksName, String keyValidator, String comparator, boolean addMetaData) throws Exception {
    CfDef cf = new CfDef(ksName,
            "OtherCf1");
    cf.setKey_validation_class(keyValidator);
    cf.setComparator_type(comparator);
    if (addMetaData) {
      cf.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("col_name_utf8"), UTF8Type.class.getName()));
      cf.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("col_name_bytes"), BytesType.class.getName()));
      cf.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("col_name_int"), IntegerType.class.getName()));
      cf.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("col_name_long"), LongType.class.getName()));
      cf.addToColumn_metadata(new ColumnDef(ByteBufferUtil.bytes("col_name_timeuuid"), TimeUUIDType.class.getName()));
    }
    KsDef ks = new KsDef(ksName,
            "org.apache.cassandra.locator.SimpleStrategy",
            Arrays.asList(cf));
    ks.setStrategy_options(KSMetaData.optsWithRF(configuration.getInt(CassandraClientHolder.CONF_PARAM_REPLICATION_FACTOR, 1)));
    return ks;
  }
}
