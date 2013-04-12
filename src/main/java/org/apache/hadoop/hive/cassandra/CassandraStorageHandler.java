package org.apache.hadoop.hive.cassandra;

import java.util.Map;
import java.util.Properties;

import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardColumnInputFormat;
import org.apache.hadoop.hive.cassandra.output.HiveCassandraOutputFormat;
import org.apache.hadoop.hive.cassandra.serde.AbstractColumnSerDe;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public class CassandraStorageHandler implements HiveStorageHandler, HiveMetaHook {

  private Configuration configuration;

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Properties tableProperties = tableDesc.getProperties();

    // Try parsing the keyspace.columnFamily
    String tableName = tableProperties.getProperty(Constants.META_TABLE_NAME);
    String dbName = tableProperties.getProperty(Constants.META_TABLE_DB);

    String keyspace = tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_KEYSPACE_NAME);
    String columnFamily = tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_CF_NAME);

    //Identify Keyspace
    if (keyspace == null) {
        keyspace = dbName;
    }

    jobProperties.put(AbstractColumnSerDe.CASSANDRA_KEYSPACE_NAME, keyspace);

    //Identify ColumnFamily
    if (columnFamily == null) {
        columnFamily = tableName;
    }

    jobProperties.put(AbstractColumnSerDe.CASSANDRA_CF_NAME, columnFamily);

    //If no column mapping has been configured, we should create the default column mapping.
    String columnInfo = tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_COL_MAPPING);
    if(columnInfo == null) {
      columnInfo = AbstractColumnSerDe.createColumnMappingString(
        tableProperties.getProperty(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS));
    }
    jobProperties.put(AbstractColumnSerDe.CASSANDRA_COL_MAPPING, columnInfo);

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_HOST) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_HOST,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_HOST, AbstractColumnSerDe.DEFAULT_CASSANDRA_HOST));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_HOST, configuration.get(AbstractColumnSerDe.CASSANDRA_HOST));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_PORT) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_PORT,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_PORT, AbstractColumnSerDe.DEFAULT_CASSANDRA_PORT));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_PORT,configuration.get(AbstractColumnSerDe.CASSANDRA_PORT));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_PARTITIONER) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_PARTITIONER,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_PARTITIONER,
          "org.apache.cassandra.dht.RandomPartitioner"));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_PARTITIONER,configuration.get(AbstractColumnSerDe.CASSANDRA_PARTITIONER));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,
              AbstractColumnSerDe.DEFAULT_CONSISTENCY_LEVEL));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL,configuration.get(AbstractColumnSerDe.CASSANDRA_CONSISTENCY_LEVEL));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE,
              Integer.toString(AbstractColumnSerDe.DEFAULT_RANGE_BATCH_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE, configuration.get(AbstractColumnSerDe.CASSANDRA_RANGE_BATCH_SIZE));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
              Integer.toString(AbstractColumnSerDe.DEFAULT_SLICE_PREDICATE_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE, configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_SIZE));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE,
              Integer.toString(AbstractColumnSerDe.DEFAULT_SPLIT_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE, configuration.get(AbstractColumnSerDe.CASSANDRA_SPLIT_SIZE));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE,
              Integer.toString(AbstractColumnSerDe.DEFAULT_BATCH_MUTATION_SIZE)));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE, configuration.get(AbstractColumnSerDe.CASSANDRA_BATCH_MUTATION_SIZE));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START, ""));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START, configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_START));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH, ""));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH, configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_FINISH));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR, ""));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR,
          configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR));
    }

    if (configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED) == null)
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED,
          tableProperties.getProperty(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED, "false"));
    }
    else
    {
      jobProperties.put(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED,
          configuration.get(AbstractColumnSerDe.CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED));
    }
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveCassandraStandardColumnInputFormat.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    public void configureInputJobProperties(TableDesc inTableDesc, Map<String, String> inStringStringMap) {
        configureTableJobProperties(inTableDesc, inStringStringMap);
    }

    public void configureOutputJobProperties(TableDesc inTableDesc, Map<String, String> inStringStringMap) {
        configureTableJobProperties(inTableDesc, inStringStringMap);
    }

    @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveCassandraOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return CassandraColumnSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
  }

  @Override
  public void setConf(Configuration arg0) {
    this.configuration = arg0;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);

    if (!isExternal) {
      throw new MetaException("Cassandra tables must be external.");
    }

    if (table.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for Cassandra.");
    }

    CassandraManager manager = new CassandraManager(table);

    try {
      //open connection to cassandra
      manager.openConnection();
      KsDef ks = manager.getKeyspaceDesc();

      //create the column family if it doesn't exist.
      manager.createCFIfNotFound(ks);
    } catch(NotFoundException e) {
      manager.createKeyspaceWithColumns();
    } finally {
      manager.closeConnection();
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    //TODO: Should this be implemented to drop the table and its data from cassandra
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    if (deleteData && !isExternal) {
      CassandraManager manager = new CassandraManager(table);

      try {
        //open connection to cassandra
        manager.openConnection();
        //drop the table
        manager.dropTable();
      } finally {
        manager.closeConnection();
      }
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // No work needed
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }
}
