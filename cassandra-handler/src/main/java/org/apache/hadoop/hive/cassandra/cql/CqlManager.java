package org.apache.hadoop.hive.cassandra.cql;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.hive.cassandra.CassandraClientHolder;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.AbstractColumnSerDe;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class to handle the transaction to cassandra backend database.
 */
public class CqlManager {
  final static public int DEFAULT_REPLICATION_FACTOR = 1;
  final static public String DEFAULT_STRATEGY = "org.apache.cassandra.locator.SimpleStrategy";

  //Cassandra Host Name
  private final String host;

  //Cassandra Host Port
  private final int port;

  //Cassandra proxy client
  private CassandraClientHolder cch;

  //Whether or not use framed connection
  private boolean framedConnection;

  //table property
  private final Table tbl;

  //key space name
  private String keyspace;

  //column family name
  private String columnFamilyName;

  /**
   * Construct a cassandra manager object from meta table object.
   */
  public CqlManager(Table tbl) throws MetaException {
    Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();

    String cassandraHost = serdeParam.get(AbstractColumnSerDe.CASSANDRA_HOST);
    if (cassandraHost == null) {
      cassandraHost = AbstractColumnSerDe.DEFAULT_CASSANDRA_HOST;
    }

    this.host = cassandraHost;

    String cassandraPortStr = serdeParam.get(AbstractColumnSerDe.CASSANDRA_PORT);
    if (cassandraPortStr == null) {
      cassandraPortStr = AbstractColumnSerDe.DEFAULT_CASSANDRA_PORT;
    }

    try {
      port = Integer.parseInt(cassandraPortStr);
    } catch (NumberFormatException e) {
      throw new MetaException(AbstractColumnSerDe.CASSANDRA_PORT + " must be a number");
    }

    this.tbl = tbl;
    init();
  }

  private void init() {
    this.keyspace = getCassandraKeyspace();
    this.columnFamilyName = getCassandraColumnFamily();
    this.framedConnection = true;
  }

  /**
   * Open connection to the cassandra server.
   *
   * @throws MetaException
   */
  public void openConnection() throws MetaException {
    try {
      cch = new CassandraProxyClient(host, port, framedConnection, true).getClientHolder();
    } catch (CassandraException e) {
      throw new MetaException("Unable to connect to the server " + e.getMessage());
    }
  }

  /**
   * Close connection to the cassandra server.
   */
  public void closeConnection() {
    if (cch != null) {
      cch.close();
    }
  }

  /**
   * Get CfDef based on the configuration in the table.
   */
  private CfDef getCfDef() throws MetaException {
    CfDef cf = new CfDef();
    cf.setKeyspace(keyspace);
    cf.setName(columnFamilyName);

    cf.setColumn_type(getColumnType());

    return cf;
  }

  /**
   * Create a keyspace with columns defined in the table.
   */
  public KsDef createKeyspaceWithColumns()
          throws MetaException {
    try {
      KsDef ks = new KsDef();
      ks.setName(getCassandraKeyspace());
      ks.setStrategy_class(getStrategy());

      if (!ks.isSetStrategy_options())
        ks.setStrategy_options(new HashMap<String, String>());

      ks.putToStrategy_options("replication_factor", Integer.toString(getReplicationFactor()));

      ks.addToCf_defs(getCfDef());

      cch.getClient().system_add_keyspace(ks);
      cch.getClient().set_keyspace(keyspace);
      return ks;
    } catch (TException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
              + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
              + e.getMessage());
    } catch (SchemaDisagreementException e) {
      throw new MetaException("Unable to create key space '" + keyspace + "'. Error:"
              + e.getMessage());
    }

  }

  /**
   * Create the column family if it doesn't exist.
   *
   * @return
   * @throws MetaException
   */
  public boolean createCFIfNotFound() throws MetaException {
    boolean cfExists = checkColumnFamily();

    if (!cfExists) {
      return (createColumnFamily() != null);
    } else {
      return cfExists;
    }
  }

  private boolean checkColumnFamily() throws MetaException {
    boolean cfExists = false;

    String getCFQuery = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='%s';";
    String finalQuery = String.format(getCFQuery, keyspace);
    try {
      CqlResult colFamilies = cch.getClient().execute_cql3_query(ByteBufferUtil.bytes(finalQuery), Compression.NONE, ConsistencyLevel.ONE);
      List<CqlRow> rows = colFamilies.getRows();

      for (CqlRow row : rows) {
        String cfName = new String(row.columns.get(0).getValue());
        if (cfName.equalsIgnoreCase(cfName)) {
          cfExists = true;
        }
      }
    } catch (UnavailableException e) {
      MetaException me = new MetaException(e.getMessage());
      me.setStackTrace(e.getStackTrace());
      throw me;
    } catch (TException e) {
      MetaException me = new MetaException(e.getMessage());
      me.setStackTrace(e.getStackTrace());
      throw me;
    } catch (InvalidRequestException e) {
      MetaException me = new MetaException(e.getMessage());
      me.setStackTrace(e.getStackTrace());
      throw me;
    } catch (TimedOutException e) {
      MetaException me = new MetaException(e.getMessage());
      me.setStackTrace(e.getStackTrace());
      throw me;
    } catch (SchemaDisagreementException e) {
      MetaException me = new MetaException(e.getMessage());
      me.setStackTrace(e.getStackTrace());
      throw me;
    }
    return cfExists;
  }

  /**
   * Create column family based on the configuration in the table.
   */
  public CfDef createColumnFamily() throws MetaException {
    CfDef cf = getCfDef();
    try {
      cch.getClient().set_keyspace(keyspace);
      cch.getClient().system_add_column_family(cf);
      return cf;
    } catch (TException e) {
      throw new MetaException("Unable to create column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to create column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    } catch (SchemaDisagreementException e) {
      throw new MetaException("Unable to create column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    }

  }

  private String getColumnType() throws MetaException {
    String prop = getPropertyFromTable(AbstractColumnSerDe.CASSANDRA_COL_MAPPING);
    List<String> mapping;
    if (prop != null) {
      mapping = AbstractColumnSerDe.parseColumnMapping(prop);
    } else {
      List<FieldSchema> schema = tbl.getSd().getCols();
      if (schema.size() == 0) {
        throw new MetaException("Can't find table column definitions");
      }

      String[] colNames = new String[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        colNames[i] = schema.get(i).getName();
      }

      String mappingStr = AbstractColumnSerDe.createColumnMappingString(colNames);
      mapping = Arrays.asList(mappingStr.split(","));
    }

    boolean hasKey = false;
    boolean hasColumn = false;
    boolean hasValue = false;
    boolean hasSubColumn = false;

    for (String column : mapping) {
      if (column.equalsIgnoreCase(AbstractColumnSerDe.CASSANDRA_KEY_COLUMN)) {
        hasKey = true;
      } else if (column.equalsIgnoreCase(AbstractColumnSerDe.CASSANDRA_COLUMN_COLUMN)) {
        hasColumn = true;
      } else if (column.equalsIgnoreCase(AbstractColumnSerDe.CASSANDRA_SUBCOLUMN_COLUMN)) {
        hasSubColumn = true;
      } else if (column.equalsIgnoreCase(AbstractColumnSerDe.CASSANDRA_VALUE_COLUMN)) {
        hasValue = true;
      } else {
        return "Standard";
      }
    }

    if (hasKey && hasColumn && hasValue) {
      if (hasSubColumn) {
        return "Super";
      } else {
        return "Standard";
      }
    } else {
      return "Standard";
    }
  }

  /**
   * Get replication factor from the table property.
   *
   * @return replication factor
   * @throws MetaException error
   */
  private int getReplicationFactor() throws MetaException {
    String prop = getPropertyFromTable(AbstractColumnSerDe.CASSANDRA_KEYSPACE_REPFACTOR);
    if (prop == null) {
      return DEFAULT_REPLICATION_FACTOR;
    } else {
      try {
        return Integer.parseInt(prop);
      } catch (NumberFormatException e) {
        throw new MetaException(AbstractColumnSerDe.CASSANDRA_KEYSPACE_REPFACTOR + " must be a number");
      }
    }
  }

  /**
   * Get replication strategy from the table property.
   *
   * @return strategy
   */
  private String getStrategy() {
    String prop = getPropertyFromTable(AbstractColumnSerDe.CASSANDRA_KEYSPACE_STRATEGY);
    if (prop == null) {
      return DEFAULT_STRATEGY;
    } else {
      return prop;
    }
  }

  /**
   * Get keyspace name from the table property.
   *
   * @return keyspace name
   */
  private String getCassandraKeyspace() {
    String tableName = getPropertyFromTable(AbstractColumnSerDe.CASSANDRA_KEYSPACE_NAME);

    if (tableName == null) {
      tableName = tbl.getDbName();
    }

    tbl.getParameters().put(AbstractColumnSerDe.CASSANDRA_KEYSPACE_NAME, tableName);

    return tableName;
  }

  /**
   * Get cassandra column family from table property.
   *
   * @return cassandra column family name
   */
  private String getCassandraColumnFamily() {
    String tableName = getPropertyFromTable(AbstractColumnSerDe.CASSANDRA_CF_NAME);

    if (tableName == null) {
      tableName = tbl.getTableName();
    }

    tbl.getParameters().put(AbstractColumnSerDe.CASSANDRA_CF_NAME, tableName);

    return tableName;
  }

  /**
   * Get the value for a given name from the table.
   * It first checks the table property. If it is not there, it checks the serde properties.
   *
   * @param columnName given name
   * @return value
   */
  private String getPropertyFromTable(String columnName) {
    String prop = tbl.getParameters().get(columnName);
    if (prop == null) {
      prop = tbl.getSd().getSerdeInfo().getParameters().get(columnName);
    }

    return prop;
  }

  /**
   * Drop the table defined in the query.
   */
  public void dropTable() throws MetaException {
    try {
      cch.getClient().system_drop_column_family(columnFamilyName);
    } catch (TException e) {
      throw new MetaException("Unable to drop column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    } catch (InvalidRequestException e) {
      throw new MetaException("Unable to drop column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    } catch (SchemaDisagreementException e) {
      throw new MetaException("Unable to drop column family '" + columnFamilyName + "'. Error:"
              + e.getMessage());
    }
  }

}
