package org.apache.hadoop.hive.cassandra.input.cql;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingRecordReader;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.CassandraPushdownPredicate;
import org.apache.hadoop.hive.cassandra.input.HiveCassandraStandardSplit;
import org.apache.hadoop.hive.cassandra.serde.cql.AbstractCqlSerDe;
import org.apache.hadoop.hive.cassandra.serde.CassandraColumnSerDe;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@SuppressWarnings("deprecation")
public class HiveCqlInputFormat extends InputFormat<MapWritable, MapWritable>
        implements org.apache.hadoop.mapred.InputFormat<MapWritable, MapWritable> {

  static final Log LOG = LogFactory.getLog(HiveCqlInputFormat.class);

  private final CqlPagingInputFormat cfif = new CqlPagingInputFormat();

  @Override
  public RecordReader<MapWritable, MapWritable> getRecordReader(InputSplit split,
                                                                JobConf jobConf, final Reporter reporter) throws IOException {
    HiveCassandraStandardSplit cassandraSplit = (HiveCassandraStandardSplit) split;

    List<String> columns = AbstractCqlSerDe.parseColumnMapping(cassandraSplit.getColumnMapping());


    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(jobConf);

    if (columns.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    ColumnFamilySplit cfSplit = cassandraSplit.getSplit();
    Job job = new Job(jobConf);

    TaskAttemptContext tac = new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()) {
      @Override
      public void progress() {
        reporter.progress();
      }
    };

    SlicePredicate predicate = new SlicePredicate();


    int iKey = columns.indexOf(AbstractCqlSerDe.CASSANDRA_KEY_COLUMN);
    predicate.setColumn_names(getColumnNames(iKey, columns, readColIDs));


    try {

      boolean wideRows = true;

      ConfigHelper.setInputColumnFamily(tac.getConfiguration(),
              cassandraSplit.getKeyspace(), cassandraSplit.getColumnFamily(), wideRows);

      ConfigHelper.setInputSlicePredicate(tac.getConfiguration(), predicate);
      ConfigHelper.setRangeBatchSize(tac.getConfiguration(), cassandraSplit.getRangeBatchSize());
      ConfigHelper.setInputRpcPort(tac.getConfiguration(), cassandraSplit.getPort() + "");
      ConfigHelper.setInputInitialAddress(tac.getConfiguration(), cassandraSplit.getHost());
      ConfigHelper.setInputPartitioner(tac.getConfiguration(), cassandraSplit.getPartitioner());
      // Set Split Size
      ConfigHelper.setInputSplitSize(tac.getConfiguration(), cassandraSplit.getSplitSize());

      LOG.info("Validators : " + tac.getConfiguration().get(CassandraColumnSerDe.CASSANDRA_VALIDATOR_TYPE));
      List<IndexExpression> indexExpr = parseFilterPredicate(jobConf);
      if (indexExpr != null) {
        //We have pushed down a filter from the Hive query, we can use this against secondary indexes
        ConfigHelper.setInputRange(tac.getConfiguration(), indexExpr);
      }

      CqlHiveRecordReader rr = new CqlHiveRecordReader(new CqlPagingRecordReader());

      rr.initialize(cfSplit, tac);

      return rr;

    } catch (Exception ie) {
      throw new IOException(ie);
    }
  }


  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    String ks = jobConf.get(AbstractCqlSerDe.CASSANDRA_KEYSPACE_NAME);
    String cf = jobConf.get(AbstractCqlSerDe.CASSANDRA_CF_NAME);
    int slicePredicateSize = jobConf.getInt(AbstractCqlSerDe.CASSANDRA_SLICE_PREDICATE_SIZE,
            AbstractCqlSerDe.DEFAULT_SLICE_PREDICATE_SIZE);
    int sliceRangeSize = jobConf.getInt(
            AbstractCqlSerDe.CASSANDRA_RANGE_BATCH_SIZE,
            AbstractCqlSerDe.DEFAULT_RANGE_BATCH_SIZE);
    int splitSize = jobConf.getInt(
            AbstractCqlSerDe.CASSANDRA_SPLIT_SIZE,
            AbstractCqlSerDe.DEFAULT_SPLIT_SIZE);
    String cassandraColumnMapping = jobConf.get(AbstractCqlSerDe.CASSANDRA_COL_MAPPING);
    int rpcPort = jobConf.getInt(AbstractCqlSerDe.CASSANDRA_PORT, 9160);
    String host = jobConf.get(AbstractCqlSerDe.CASSANDRA_HOST);
    String partitioner = jobConf.get(AbstractCqlSerDe.CASSANDRA_PARTITIONER);

    if (cassandraColumnMapping == null) {
      throw new IOException("cassandra.columns.mapping required for Cassandra Table.");
    }

    SliceRange range = new SliceRange();
    range.setStart(new byte[0]);
    range.setFinish(new byte[0]);
    range.setReversed(false);
    range.setCount(slicePredicateSize);
    SlicePredicate predicate = new SlicePredicate();
    predicate.setSlice_range(range);

    ConfigHelper.setInputRpcPort(jobConf, "" + rpcPort);
    ConfigHelper.setInputInitialAddress(jobConf, host);
    ConfigHelper.setInputPartitioner(jobConf, partitioner);
    ConfigHelper.setInputSlicePredicate(jobConf, predicate);
    ConfigHelper.setInputColumnFamily(jobConf, ks, cf);
    ConfigHelper.setRangeBatchSize(jobConf, sliceRangeSize);
    ConfigHelper.setInputSplitSize(jobConf, splitSize);

    Job job = new Job(jobConf);
    JobContext jobContext = new JobContext(job.getConfiguration(), job.getJobID());

    Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);
    List<org.apache.hadoop.mapreduce.InputSplit> splits = getSplits(jobContext);
    InputSplit[] results = new InputSplit[splits.size()];

    for (int i = 0; i < splits.size(); ++i) {
      HiveCassandraStandardSplit csplit = new HiveCassandraStandardSplit(
              (ColumnFamilySplit) splits.get(i), cassandraColumnMapping, tablePaths[0]);
      csplit.setKeyspace(ks);
      csplit.setColumnFamily(cf);
      csplit.setRangeBatchSize(sliceRangeSize);
      csplit.setSplitSize(splitSize);
      csplit.setHost(host);
      csplit.setPort(rpcPort);
      csplit.setSlicePredicateSize(slicePredicateSize);
      csplit.setPartitioner(partitioner);
      csplit.setColumnMapping(cassandraColumnMapping);
      results[i] = csplit;
    }
    return results;
  }

  /**
   * Return a list of columns names to read from cassandra. The column defined as the key in the
   * column mapping
   * should be skipped.
   *
   * @param iKey       the index of the key defined in the column mappping
   * @param columns    column mapping
   * @param readColIDs column names to read from cassandra
   */
  private List<ByteBuffer> getColumnNames(int iKey, List<String> columns, List<Integer> readColIDs) {

    List<ByteBuffer> results = new ArrayList();
    int maxSize = columns.size();

    for (Integer i : readColIDs) {
      assert (i < maxSize);
      if (i != iKey) {
        results.add(ByteBufferUtil.bytes(columns.get(i.intValue())));
      }
    }

    return results;
  }

  @Override
  public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context)
          throws IOException {
    return cfif.getSplits(context);
  }


  @Override
  public org.apache.hadoop.mapreduce.RecordReader<MapWritable, MapWritable> createRecordReader(
          org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext tac) throws IOException,
          InterruptedException {

    return new CqlHiveRecordReader(new CqlPagingRecordReader());
  }

  /**
   * Look for a filter predicate pushed down by the StorageHandler. If a filter was pushed
   * down, the filter expression and the list of indexed columns should be set in the
   * JobConf properties. If either is not set, we can't deal with the filter here so return
   * null. If both are present in the JobConf, translate the filter expression into a list of C*
   * IndexExpressions which we'll later use in queries. The filter expression should translate exactly
   * to IndexExpressions, as our HiveStoragePredicateHandler implementation has already done this
   * once. As an additional check, if this is no longer the case & there is some residual predicate
   * after translation, throw an Exception.
   *
   * @param jobConf Job Configuration
   * @return C* IndexExpressions representing the pushed down filter or null pushdown is not possible
   * @throws java.io.IOException if there are problems deserializing from the JobConf
   */
  private List<IndexExpression> parseFilterPredicate(JobConf jobConf) throws IOException {
    String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      return null;
    }

    ExprNodeDesc filterExpr = Utilities.deserializeExpression(filterExprSerialized, jobConf);
    String encodedIndexedColumns = jobConf.get(AbstractCqlSerDe.CASSANDRA_INDEXED_COLUMNS);
    Set<ColumnDef> indexedColumns = CassandraPushdownPredicate.deserializeIndexedColumns(encodedIndexedColumns);
    if (indexedColumns.isEmpty()) {
      return null;
    }

    IndexPredicateAnalyzer analyzer = CassandraPushdownPredicate.newIndexPredicateAnalyzer(indexedColumns);
    List<IndexSearchCondition> searchConditions = new ArrayList<IndexSearchCondition>();
    ExprNodeDesc residualPredicate = analyzer.analyzePredicate(filterExpr, searchConditions);

    // There should be no residual predicate since we already negotiated
    // that earlier in CqlStorageHandler.decomposePredicate.
    if (residualPredicate != null) {
      throw new RuntimeException("Unexpected residual predicate : " + residualPredicate.getExprString());
    }

    if (!searchConditions.isEmpty()) {
      return CassandraPushdownPredicate.translateSearchConditions(searchConditions, indexedColumns);
    } else {
      throw new RuntimeException("At least one search condition expected in filter predicate");
    }
  }
}
