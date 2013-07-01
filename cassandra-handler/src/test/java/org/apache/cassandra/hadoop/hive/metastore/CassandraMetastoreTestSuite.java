package org.apache.cassandra.hadoop.hive.metastore;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CassandraHiveMetaStoreTest.class,
        MetaStorePersisterTest.class,
        SchemaManagerServiceTest.class
})
public class CassandraMetastoreTestSuite {
}
