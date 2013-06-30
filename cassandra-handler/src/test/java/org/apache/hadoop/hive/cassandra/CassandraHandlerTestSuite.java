package org.apache.hadoop.hive.cassandra;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestCassandraProxyClient.class,
        CassandraPushdownPredicateTest.class,
        CassandraClientHolderTest.class})
public class CassandraHandlerTestSuite {
}
