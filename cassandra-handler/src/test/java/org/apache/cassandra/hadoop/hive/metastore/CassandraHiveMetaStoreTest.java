package org.apache.cassandra.hadoop.hive.metastore;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Test plumbing of CassandraHiveMetaStore
 * 
 * @author zznate
 */
public class CassandraHiveMetaStoreTest extends MetaStoreTestBase {

    @Test
    public void testSetConf() 
    {
        // INIT procedure from HiveMetaStore:
        //empty c-tor
        // setConf
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());        
    }
    
    @Test(expected=CassandraHiveMetaStoreException.class)
    public void testSetConfConnectionFail() 
    {
        // INIT procedure from HiveMetaStore:
        //empty c-tor
        // setConf
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        // Throw a runtime exception if we can ping cassandra?
        Configuration conf = buildConfiguration();
        conf.setInt("cassandra.connection.port",8181);
        metaStore.setConf(conf);        
    }
    
    @Test
    public void testCreateDeleteDatabaseAndTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("db_name");
        assertEquals(database, foundDatabase);
        
        Table table = new Table();
        table.setDbName("db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("db_name", "table_name");
        assertEquals(table, foundTable);
        
        assertEquals(1,metaStore.getAllDatabases().size());
        assertEquals(1,metaStore.getAllTables("db_name").size());
        
        metaStore.dropTable("db_name", "table_name");
        assertNull(metaStore.getTable("db_name", "table_name"));
        assertEquals(0,metaStore.getAllTables("db_name").size());
    }
    
    @Test
    public void testFindEmptyPatitionList() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("db_name");
        assertEquals(database, foundDatabase);
        
        Table table = new Table();
        table.setDbName("db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        List<String> partitionNames = metaStore.listPartitionNames("db_name", "table_name", (short) 100);
        assertEquals(0, partitionNames.size());
        partitionNames = metaStore.listPartitionNames("db_name", "table_name", (short) -1);
        assertEquals(0, partitionNames.size());
    }
    
    @Test
    public void testAlterTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        
        Table table = new Table();
        table.setDbName("alter_table_db_name");
        table.setTableName("orig_table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("alter_table_db_name", "orig_table_name");
        assertEquals(table, foundTable);
        
        Table altered = new Table();
        altered.setDbName("alter_table_db_name");
        altered.setTableName("new_table_name");
               
        metaStore.alterTable("alter_table_db_name", "orig_table_name", altered);
        assertEquals(1,metaStore.getAllTables("alter_table_db_name").size());
        
    }
    
    @Test
    public void testAlterDatabaseTable() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("alter_db_db_name", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        
        Table table = new Table();
        table.setDbName("alter_db_db_name");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("alter_db_db_name", "table_name");
        assertEquals(table, foundTable);
        Database altered = new Database("alter_db_db_name2", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.alterDatabase("alter_db_db_name", altered);
        
        assertEquals(1,metaStore.getAllTables("alter_db_db_name2").size());
        
    }
    
    @Test
    public void testAddParition() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("alter_part_db", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("alter_part_db");
        assertEquals(database, foundDatabase);
        
        Table table = new Table();
        table.setDbName("alter_part_db");
        table.setTableName("table_name");
        metaStore.createTable(table);
        
        Partition part = new Partition();        
        part.setDbName("alter_part_db");
        part.setTableName("table_name");
        List<String> partValues = new ArrayList<String>();
        partValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-08");
        partValues.add("cfs://localhost:9160/user/hive/warehouse/mydb.db/invites/ds=2008-08-15");
        part.setValues(partValues);
        metaStore.addPartition(part);

        
        Partition foundPartition = metaStore.getPartition("alter_part_db", "table_name", partValues);
        assertEquals(part,foundPartition);

    }
    
    @Test
    public void testCreateMultipleDatabases() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("db_1", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        database.setName("db_2");
        metaStore.createDatabase(database);
        assertTrue(metaStore.getAllDatabases().size() > 1);
    }
    
    @Test
    public void testAddDropReAddDatabase() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("add_drop_readd_db", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDatabase = metaStore.getDatabase("add_drop_readd_db");
        assertEquals(database, foundDatabase);
        metaStore.dropDatabase("add_drop_readd_db");
        try {
            foundDatabase = metaStore.getDatabase("add_drop_readd_db");
            fail();
        } catch (NoSuchObjectException nsoe) {
            foundDatabase = null;
        }
        metaStore.createDatabase(database);
        foundDatabase = metaStore.getDatabase("add_drop_readd_db");
        assertEquals(database, foundDatabase);        
    }
    
    @Test
    public void testCaseInsensitiveNaming() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        metaStore.setConf(buildConfiguration());
        Database database = new Database("CiDbNaMe", "My Database", "file:///tmp/", new HashMap<String, String>());
        metaStore.createDatabase(database);
        Database foundDb = metaStore.getDatabase("cidbname");
        assertNotNull(foundDb);
        Table table = new Table();
        table.setDbName("cidbname");
        table.setTableName("TaBlE");
        metaStore.createTable(table);
        
        Table foundTable = metaStore.getTable("cidBname", "table");        
        assertNotNull(foundTable);
        
    }
    
    @Test
    public void testAutoCreateFromKeyspace() throws Exception 
    {
        CassandraHiveMetaStore metaStore = new CassandraHiveMetaStore();
        Configuration conf = buildConfiguration();
        CassandraClientHolder clientHolder = new CassandraClientHolder(conf);
        KsDef ksDef = setupOtherKeyspace(conf,"AutoCreatedFromKeyspace", false);
        clientHolder.getClient().system_add_keyspace(ksDef);
        conf.setBoolean("cassandra.autoCreateHiveSchema", true);
        metaStore.setConf(conf);        
        Database foundDb = metaStore.getDatabase("AutoCreatedFromKeyspace");
        assertNotNull(foundDb);
        CfDef cf = new CfDef("AutoCreatedFromKeyspace","OtherCf2");
        cf.setKey_validation_class("UTF8Type");
        cf.setComparator_type("UTF8Type");
        clientHolder.getClient().set_keyspace("HiveMetaStore");
        clientHolder.getClient().system_add_column_family(cf);
        metaStore.getAllTables("AutoCreatedFromKeyspace");
        assertNotNull(metaStore.getTable("AutoCreatedFromKeyspace", "OtherCf2"));
    }
    
   
}
