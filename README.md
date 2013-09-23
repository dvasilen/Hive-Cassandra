Hive-Cassandra
==============

1. Hive Storage Handler for Cassandra (cloned from https://github.com/riptano/hive/tree/hive-0.8.1-merge/cassandra-handler)
2. git pull https://github.com/milliondreams/hive.git cas-support-cql
3. Updated for Hive 0.11.0 and Cassandra 1.2.9

Cassandra CLI Example
==============

./cassandra-cli

create keyspace test
with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'
and strategy_options = [{replication_factor:1}];

create column family users with comparator = UTF8Type;

update column family users with
        column_metadata =
        [
        {column_name: first, validation_class: UTF8Type},
        {column_name: last, validation_class: UTF8Type},
        {column_name: age, validation_class: UTF8Type, index_type: KEYS}
        ];

assume users keys as utf8;

set users['jsmith']['first'] = 'John';
set users['jsmith']['last'] = 'Smith';
set users['jsmith']['age'] = '38';
set users['jdoe']['first'] = 'John';
set users['jdoe']['last'] = 'Dow';
set users['jdoe']['age'] = '42';

get users['jdoe'];

./hive 

DROP TABLE IF EXISTS cassandra_users;
CREATE EXTERNAL TABLE cassandra_users  (key string, first string, last string, age string)
STORED BY 'org.apache.hadoop.hive.cassandra.CassandraStorageHandler'
WITH SERDEPROPERTIES("cassandra.cf.name" = "users","cassandra.host"="9.30.214.79","cassandra.port" = "9160")
TBLPROPERTIES ("cassandra.ks.name" = "test");

select * from cassandra_users;

See http://www.datastax.com/docs/datastax_enterprise3.0/solutions/about_hive for other SERDE and TABLE properties.

