package org.apache.hadoop.hive.cassandra.serde.cql;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cassandra.input.cql.LazyCqlRow;
import org.apache.hadoop.hive.cassandra.output.CassandraPut;
import org.apache.hadoop.hive.cassandra.serde.RegularTableMapping;
import org.apache.hadoop.hive.cassandra.serde.TableMapping;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public abstract class AbstractCqlSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(AbstractCqlSerDe.class.getName());

  public static final String CASSANDRA_KEYSPACE_NAME = "cassandra.ks.name"; // keyspace
  public static final String CASSANDRA_KEYSPACE_REPFACTOR = "cassandra.ks.repfactor"; //keyspace replication factor
  public static final String CASSANDRA_KEYSPACE_STRATEGY = "cassandra.ks.strategy"; //keyspace replica placement strategy

  public static final String CASSANDRA_CF_NAME = "cassandra.cf.name"; // column family
  public static final String CASSANDRA_CF_COUNTERS = "cassandra.cf.counters"; // flag this as a counter CF
  public static final String CASSANDRA_RANGE_BATCH_SIZE = "cassandra.range.size";
  public static final String CASSANDRA_SLICE_PREDICATE_SIZE = "cassandra.slice.predicate.size";
  public static final String CASSANDRA_SPLIT_SIZE = "cassandra.input.split.size";
  public static final String CASSANDRA_HOST = "cassandra.host"; // initialHost
  public static final String CASSANDRA_PORT = "cassandra.port"; // rcpPort
  public static final String CASSANDRA_PARTITIONER = "cassandra.partitioner"; // partitioner
  public static final String CASSANDRA_COL_MAPPING = "cassandra.columns.mapping";
  public static final String CASSANDRA_INDEXED_COLUMNS = "cassandra.indexed.columns";
  public static final String CASSANDRA_BATCH_MUTATION_SIZE = "cassandra.batchmutate.size";
  public static final String CASSANDRA_SLICE_PREDICATE_COLUMN_NAMES = "cassandra.slice.predicate.column_names";
  public static final String CASSANDRA_SLICE_PREDICATE_RANGE_START = "cassandra.slice.predicate.range.start";
  public static final String CASSANDRA_SLICE_PREDICATE_RANGE_FINISH = "cassandra.slice.predicate.range.finish";
  public static final String CASSANDRA_SLICE_PREDICATE_RANGE_COMPARATOR = "cassandra.slice.predicate.range.comparator";
  public static final String CASSANDRA_SLICE_PREDICATE_RANGE_REVERSED = "cassandra.slice.predicate.range.reversed";
  public static final String CASSANDRA_SLICE_PREDICATE_RANGE_COUNT = "cassandra.slice.predicate.range.count";
  public static final String CASSANDRA_ENABLE_WIDEROW_ITERATOR = "cassandra.enable.widerow.iterator";

  public static final String CASSANDRA_SPECIAL_COLUMN_KEY = "row_key";
  public static final String CASSANDRA_SPECIAL_COLUMN_COL = "column_name";
  public static final String CASSANDRA_SPECIAL_COLUMN_SCOL = "sub_column_name";
  public static final String CASSANDRA_SPECIAL_COLUMN_VAL = "value";

  public static final String CASSANDRA_KEY_COLUMN = ":key";
  public static final String CASSANDRA_COLUMN_COLUMN = ":column";
  public static final String CASSANDRA_SUBCOLUMN_COLUMN = ":subcolumn";
  public static final String CASSANDRA_VALUE_COLUMN = ":value";

  public static final String CASSANDRA_CONSISTENCY_LEVEL = "cassandra.consistency.level";
  public static final String CASSANDRA_THRIFT_MODE = "cassandra.thrift.mode";

  public static final int DEFAULT_SPLIT_SIZE = 64 * 1024;
  public static final int DEFAULT_RANGE_BATCH_SIZE = 1000;
  public static final int DEFAULT_SLICE_PREDICATE_SIZE = 1000;
  public static final String DEFAULT_CASSANDRA_HOST = "localhost";
  public static final String DEFAULT_CASSANDRA_PORT = "9160";
  public static final String DEFAULT_CONSISTENCY_LEVEL = "ONE";
  public static final int DEFAULT_BATCH_MUTATION_SIZE = 500;
  public static final String DELIMITER = ",";

  /* names of columns from SerdeParameters */
  protected List<String> cassandraColumnNames;
  /* index of key column in results */
  protected int iKey;
  protected TableMapping mapping;

  protected ObjectInspector cachedObjectInspector;
  protected SerDeParameters serdeParams;
  protected LazyCqlRow lazyCqlRow;
  protected String cassandraKeyspace;
  protected String cassandraColumnFamily;
  protected List<Text> cassandraColumnNamesText;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    initCassandraSerDeParameters(conf, tbl, getClass().getName());
    cachedObjectInspector = createObjectInspector();

    lazyCqlRow = new LazyCqlRow(
            (LazySimpleStructObjectInspector) cachedObjectInspector);

    if (LOG.isDebugEnabled()) {
      LOG.debug("CassandraSerDe initialized with : columnNames = "
              + StringUtils.join(serdeParams.getColumnNames(), ",")
              + " columnTypes = "
              + StringUtils.join(serdeParams.getColumnTypes(), ",")
              + " cassandraColumnMapping = "
              + cassandraColumnNames);
    }

  }

  /**
   * Create the object inspector.
   *
   * @return object inspector
   */
  protected abstract ObjectInspector createObjectInspector();

  /*
   *
   * @see org.apache.hadoop.hive.serde2.Deserializer#deserialize(org.apache.hadoop.io.Writable)
   * Turns a Cassandra row into a Hive row.
   */
  @Override
  public Object deserialize(Writable w) throws SerDeException {
    if (!(w instanceof MapWritable)) {
      throw new SerDeException(getClass().getName() + ": expects MapWritable not " + w.getClass().getName());
    }

    MapWritable columnMap = (MapWritable) w;
    lazyCqlRow.init(columnMap, cassandraColumnNames, cassandraColumnNamesText);
    return lazyCqlRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return CassandraPut.class;
  }

  /*
   * Turns obj (a Hive Row) into a cassandra data format.
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
              + " can only serialize struct types, but we got: "
              + objInspector.getTypeName());
    }
    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields =
            (serdeParams.getRowTypeInfo() != null &&
                    ((StructTypeInfo) serdeParams.getRowTypeInfo())
                            .getAllStructFieldNames().size() > 0) ?
                    ((StructObjectInspector) getObjectInspector()).getAllStructFieldRefs()
                    : null;
    try {
      assert iKey >= 0;
      return mapping.getWritable(fields, list, declaredFields);
    } catch (IOException e) {
      throw new SerDeException("Unable to serialize this object! " + e);
    }
  }


  protected abstract void initCassandraSerDeParameters(Configuration job, Properties tbl, String serdeName)
          throws SerDeException;

  /**
   * Parses the cassandra columns mapping to identify the column name.
   * One of the Hive table columns maps to the cassandra row key, by default the
   * first column.
   *
   * @param columnMapping - the column mapping specification to be parsed
   * @return a list of cassandra column names
   */
  public static List<String> parseColumnMapping(String columnMapping) {
    assert StringUtils.isNotBlank(columnMapping);
    String[] columnArray = columnMapping.split(",");
    String[] trimmedColumnArray = trim(columnArray);

    List<String> columnList = Arrays.asList(trimmedColumnArray);

    int iKey = columnList.indexOf(CASSANDRA_KEY_COLUMN);

    if (iKey == -1) {
      columnList = new ArrayList<String>(columnList);
      columnList.add(0, CASSANDRA_KEY_COLUMN);
    }

    return columnList;
  }

  /**
   * Return the column mapping created from column names.
   *
   * @param colNames column names in array format
   * @return column mapping string
   */
  public static String createColumnMappingString(String[] colNames) {

    //Regular non-transposed logic. The first column maps to the key automatically.
    StringBuilder mappingStr = new StringBuilder(CASSANDRA_KEY_COLUMN);
    //StringBuilder mappingStr = new StringBuilder(); //Since CQL doesn't have the special :key column
    for (int i = 1; i < colNames.length; i++) {
      mappingStr.append(",");
      mappingStr.append(colNames[i]);
    }

    return mappingStr.toString();
  }

  /*
   * Creates the cassandra column mappings from the hive column names.
   * This would be triggered when no cassandra.columns.mapping has been defined
   * in the user query.
   *
   * row_key is a special column name, it maps to the key of a row in cassandra;
   * column_name maps to the name of a column/supercolumn;
   * value maps to the value of a column;
   * sub_column_name maps to the name of a column (This can only be used for a super column family.)
   *
   * @param tblColumnStr hive table column names
   */
  public static String createColumnMappingString(String tblColumnStr) {
    if (StringUtils.isBlank(tblColumnStr)) {
      throw new IllegalArgumentException("table must have columns");
    }

    //String[] colNames = tblColumnStr.split(",");

    //return createColumnMappingString(colNames);
    return tblColumnStr;
  }

  /**
   * Parse cassandra keyspace from table properties.
   *
   * @param tbl table properties
   * @return cassandra keyspace
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *          error parsing keyspace
   */
  protected String parseCassandraKeyspace(Properties tbl) throws SerDeException {
    String result = tbl.getProperty(CASSANDRA_KEYSPACE_NAME);

    if (result == null) {

      result = tbl
              .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);

      if (result == null) {
        throw new SerDeException("CassandraKeyspace not defined" + tbl.toString());
      }

      if (result.indexOf(".") != -1) {
        result = result.substring(0, result.indexOf("."));
      }
    }

    return result;
  }

  /**
   * Parse cassandra column family name from table properties.
   *
   * @param tbl table properties
   * @return cassandra column family name
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *          error parsing column family name
   */
  protected String parseCassandraColumnFamily(Properties tbl) throws SerDeException {
    String result = tbl.getProperty(CASSANDRA_CF_NAME);

    if (result == null) {

      result = tbl
              .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME);

      if (result == null) {
        throw new SerDeException("CassandraColumnFamily not defined" + tbl.toString());
      }

      if (result.indexOf(".") != -1) {
        result = result.substring(result.indexOf(".") + 1);
      }
    }

    return result;
  }

  /**
   * Parse the column mappping from table properties. If cassandra.columns.mapping
   * is defined in the property, use it to create the mapping. Otherwise, create the mapping from table
   * columns using the default mapping.
   *
   * @param tbl table properties
   * @return A list of column names
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *
   */
  protected List<String> parseOrCreateColumnMapping(Properties tbl) throws SerDeException {
    String prop = tbl.getProperty(CASSANDRA_COL_MAPPING);

    if (prop != null) {
      return parseColumnMapping(prop);
    } else {
      String tblColumnStr = tbl.getProperty(Constants.LIST_COLUMNS);

      if (tblColumnStr != null) {
        //auto-create
        String mappingStr = createColumnMappingString(tblColumnStr);

        if (LOG.isDebugEnabled()) {
          LOG.debug("table column string: " + tblColumnStr);
          LOG.debug("Auto-created mapping string: " + mappingStr);
        }

        return Arrays.asList(mappingStr.split(","));

      } else {
        throw new SerDeException("Can't find table column definitions");
      }
    }
  }

  /**
   * Set the table mapping. We only support transposed mapping and regular table mapping for now.
   *
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   *
   */
  protected void setTableMapping() throws SerDeException {
    mapping = new RegularTableMapping(cassandraColumnFamily, cassandraColumnNames, serdeParams);
  }

  /**
   * Trim the white spaces, new lines from the input array.
   *
   * @param input a input string array
   * @return a trimmed string array
   */
  protected static String[] trim(String[] input) {
    String[] trimmed = new String[input.length];
    for (int i = 0; i < input.length; i++) {
      trimmed[i] = input[i].trim();
    }

    return trimmed;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * @return the name of the cassandra keyspace as parsed from table properties
   */
  public String getCassandraKeyspace() {
    return cassandraKeyspace;
  }

  /**
   * @return the name of the cassandra columnfamily as parsed from table properties
   */
  public String getCassandraColumnFamily() {
    return cassandraColumnFamily;
  }
}
