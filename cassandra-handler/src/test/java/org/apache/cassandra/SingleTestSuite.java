package org.apache.cassandra;

import org.apache.cassandra.hadoop.fs.CassandraFileSystemTest;
import org.apache.cassandra.hadoop.fs.CassandraOutputStreamTest;
import org.apache.cassandra.hadoop.fs.INodeTest;
import org.apache.cassandra.hadoop.hive.metastore.CassandraHiveMetaStoreTest;
import org.apache.cassandra.hadoop.hive.metastore.MetaStorePersisterTest;
import org.apache.cassandra.hadoop.hive.metastore.SchemaManagerServiceTest;
import org.apache.hadoop.hive.cassandra.CassandraClientHolderTest;
import org.apache.hadoop.hive.cassandra.CassandraPushdownPredicateTest;
import org.apache.hadoop.hive.cassandra.TestCassandraProxyClient;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestCassandraProxyClient.class,
        CassandraPushdownPredicateTest.class,
        CassandraClientHolderTest.class,
        INodeTest.class,
        CassandraFileSystemTest.class,
        CassandraOutputStreamTest.class,
        CassandraHiveMetaStoreTest.class,
        MetaStorePersisterTest.class,
        SchemaManagerServiceTest.class
})
public class SingleTestSuite {
}