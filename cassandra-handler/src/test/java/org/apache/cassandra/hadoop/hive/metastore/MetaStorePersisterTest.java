/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.hadoop.hive.metastore;

import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;

public class MetaStorePersisterTest extends MetaStoreTestBase
{

    private MetaStorePersister metaStorePersister;
    private CassandraClientHolder clientHolder;
    private CassandraHiveMetaStore metaStore;
    
    
    public void setupClient() throws Exception 
    {
        Configuration conf = buildConfiguration();
        if ( metaStore == null ) 
        {
            metaStore = new CassandraHiveMetaStore();
            metaStore.setConf(conf);        
        }
        metaStorePersister = new MetaStorePersister(conf);
    }
    
    @Test
    public void testBasicPersistMetaStoreEntity() throws Exception 
    {
        setupClient();
        Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("uri");
        database.setParameters(new HashMap<String, String>());
        metaStorePersister.save(database.metaDataMap, database, database.getName()); // save(TBase base).. via _Fields and findByThriftId, publi MetaDataMap
    }
    
    @Test(expected=NotFoundException.class)
    public void testEntityNotFound() throws Exception
    {
        setupClient();
        Database database = new Database();
        database.setName("foo");
        metaStorePersister.load(database, "name");
    }
    
    @Test
    public void testBasicLoadMetaStoreEntity() throws Exception 
    {
        setupClient();
        Database database = new Database();
        database.setName("name");
        database.setDescription("description");
        database.setLocationUri("uri");
        database.setParameters(new HashMap<String, String>());
        metaStorePersister.save(database.metaDataMap, database, database.getName());
        Database foundDb = new Database();
        foundDb.setName("name");
        metaStorePersister.load(foundDb, "name");
        assertEquals(database, foundDb);
    }
    
    @Test
    public void testFindMetaStoreEntities() throws Exception  
    {
        setupClient();
        Database database = new Database();
        database.setName("dbname");
        database.setDescription("description");
        database.setLocationUri("uri");
        database.setParameters(new HashMap<String, String>());
        metaStorePersister.save(database.metaDataMap, database, database.getName());
        Table table = new Table();
        table.setDbName("dbname");
        table.setTableName("table_one");
        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        table.setTableName("table_two");
        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        table.setTableName("table_three");
        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        table.setTableName("other_table");
        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        
        List tables = metaStorePersister.find(table, "dbname");
        assertEquals(4, tables.size());
        tables = metaStorePersister.find(table, "dbname", "table", 100);
        assertEquals(3, tables.size());
    }
    
    @Test
    public void testEntityDeletion() throws Exception 
    {
        setupClient();
        Database database = new Database();
        database.setName("dbname");
        database.setDescription("description");
        database.setLocationUri("uri");
        database.setParameters(new HashMap<String, String>());
        metaStorePersister.save(database.metaDataMap, database, database.getName());
        
        Table table = new Table();
        table.setDbName("dbname");
        table.setTableName("table_one");
        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
        
        Database foundDb = new Database();
        foundDb.setName("dbname");
        metaStorePersister.load(foundDb, "dbname");
        assertEquals(database, foundDb);
        Table foundTable = new Table();
        foundTable.setDbName(table.getDbName());
        foundTable.setTableName(table.getTableName());
        
        metaStorePersister.load(foundTable, "dbname");
        
        assertEquals(table, foundTable);
        
        metaStorePersister.remove(foundTable, "dbname");
        metaStorePersister.remove(foundDb, "dbname");
        try {
            metaStorePersister.load(foundTable, "dbname");
            fail();
            metaStorePersister.load(foundDb, "dbname");
            fail();
        } 
        catch (NotFoundException e) 
        {
            // win! \o/
        }
    }
    

}
