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

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.apache.hadoop.hive.cassandra.serde.AbstractColumnSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.model.*;
import org.apache.thrift.TBase;

/**
 * Serializes thrift structs for Hive Meta Store to Apache Cassandra.
 *
 * All of the 'entities' in the meta store schema go into a single row for
 * the database for which they belong.
 *
 * Database names are stored in a special row with the key '__databases__'
 *
 * Meta information such as roles and privileges (that is, 'entities' that
 * can be cross-database) go into the row with the key '__meta__'
 *
 */
public class CassandraHiveMetaStore implements RawStore {

    private static final Logger log = LoggerFactory.getLogger(CassandraHiveMetaStore.class);
    private Configuration configuration;
    private MetaStorePersister metaStorePersister;
    //private CassandraClientHolder cassandraClientHolder;
    private SchemaManagerService schemaManagerService;

    public CassandraHiveMetaStore()
    {
        log.debug("Creating CassandraHiveMetaStore");
    }

    /**
     * Starts the underlying Cassandra.Client as well as creating
     * the meta store schema if it does not already exist and
     * creating schemas for keyspaces found if 'cassandra.autoCreateSchema'
     * is set to true.
     */
    public void setConf(Configuration conf)
    {
        configuration = conf;
        //cassandraClientHolder = new CassandraClientHolder(configuration);

      /* final String cassandraHost = conf.get(AbstractColumnSerDe.CASSANDRA_HOST);
      final int cassandraPort = Integer.parseInt(conf.get(AbstractColumnSerDe.CASSANDRA_PORT));

      final CassandraProxyClient client;
      try {
        client = new CassandraProxyClient(
                cassandraHost, cassandraPort, true, true);
      } catch (CassandraException e) {
        throw new IOException(e);
      }  */


      schemaManagerService = new SchemaManagerService(this, configuration);
        // create the meta store if it does not exist
        schemaManagerService.createMetaStoreIfNeeded();
        // load meta store
        metaStorePersister = new MetaStorePersister(configuration);
        // create schemas for Keyspaces if configured for such
        schemaManagerService.createKeyspaceSchemasIfNeeded();


    }

    public Configuration getConf()
    {
        return configuration;
    }


    public void createDatabase(Database database) throws InvalidObjectException, MetaException
    {
        log.debug("createDatabase with {}", database);

        metaStorePersister.save(database.metaDataMap, database, database.getName());
        metaStorePersister.save(database.metaDataMap, database, CassandraClientHolder.DATABASES_ROW_KEY);
    }


    public Database getDatabase(String databaseName) throws NoSuchObjectException
    {
        log.debug("in getDatabase with database name: {}", databaseName);
        Database db = new Database();
        db.setName(databaseName);
        try
        {
            metaStorePersister.load(db, databaseName);
        }
        catch (NotFoundException e)
        {
            if ( schemaManagerService.getAutoCreateSchema() && maybeAutoCreateFromCassandra(databaseName) )
            {
                log.debug("Configured for auto schema creation with keyspace found: {}", databaseName);
                try
                {
                    metaStorePersister.load(db, databaseName);
                }
                catch (NotFoundException nfe)
                {
                    throw new CassandraHiveMetaStoreException("Could not auto create schema.", nfe);
                }
            }
            else
            {
                throw new NoSuchObjectException("Database named " + databaseName + " did not exist.");
            }
        }
        return db;
    }

    private boolean maybeAutoCreateFromCassandra(String databaseName)
    {
        KsDef ksDef = schemaManagerService.getKeyspaceForDatabaseName(databaseName);
        if (ksDef != null)
        {
            log.debug("Found mapped Keyspace {} in Cassandra. Creating schema.", databaseName);
            schemaManagerService.createKeyspaceSchema(ksDef);
            return true;
        }
        log.debug("Did not find matching keyspace for {} database", databaseName);
        return false;
    }

    public List<String> getDatabases(String databaseNamePattern) throws MetaException
    {
        log.debug("in getDatabases with databaseNamePattern: {}", databaseNamePattern);
        List<TBase> databases = metaStorePersister.find(new Database(),
                CassandraClientHolder.DATABASES_ROW_KEY, databaseNamePattern,100);
        List<String> results = new ArrayList<String>(databases.size());
        for (TBase tBase : databases)
        {
            Database db = (Database)tBase;
            if ( StringUtils.isEmpty(databaseNamePattern) || db.getName().matches(databaseNamePattern) )
                results.add(db.getName());
        }
        return results;
    }

    public boolean alterDatabase(String oldDatabaseName, Database database)
            throws NoSuchObjectException, MetaException
    {
        try
        {
            createDatabase(database);
        }
        catch (InvalidObjectException e)
        {
            throw new CassandraHiveMetaStoreException("Error attempting to alter database: " + oldDatabaseName, e);
        }
        List<String> tables = getAllTables(oldDatabaseName);
        List<TBase> removeable = new ArrayList<TBase>();
        for (String table : tables)
        {
            Table t = getTable(oldDatabaseName, table);
            try
            {
                Table nTable = t.deepCopy();
                nTable.setDbName(database.getName());

                removeable.addAll(updateTableComponents(oldDatabaseName, database, t.getTableName(), t));
                createTable(nTable);


            }
            catch (Exception e)
            {
                throw new MetaException("Problem in database rename");
            }
            removeable.add(t);
        }
        metaStorePersister.removeAll(removeable, oldDatabaseName);

        return true;
    }

    public boolean dropDatabase(String databaseName) throws NoSuchObjectException,
            MetaException
    {
        Database database = new Database();
        database.setName(databaseName);
        metaStorePersister.remove(database, databaseName);
        metaStorePersister.remove(database, CassandraClientHolder.DATABASES_ROW_KEY);
        return true;
    }

    public List<String> getAllDatabases() throws MetaException
    {
        return getDatabases(StringUtils.EMPTY);
    }


    public void createTable(Table table) throws InvalidObjectException, MetaException
    {

        metaStorePersister.save(table.metaDataMap, table, table.getDbName());
    }

    public Table getTable(String databaseName, String tableName) throws MetaException
    {
        log.debug("in getTable with database name: {} and table name: {}", databaseName, tableName);
        Table table = new Table();
        table.setTableName(tableName);
        try
        {
            metaStorePersister.load(table, databaseName);
        }
        catch (NotFoundException e)
        {
            //throw new MetaException("Table: " + tableName + " did not exist in database: " + databaseName);
            return null;
        }
        return table;
    }

    /**
     * Retrieve the tables for the given database and pattern.
     *
     * @param dbName
     * @param tableNamePattern the pattern passed as is to {@link String#matches(String)} of
     * {@link org.apache.hadoop.hive.metastore.api.Table#getTableName()}
     */
    public List<String> getTables(String dbName, String tableNamePattern)
    throws MetaException
    {
        log.info("in getTables with dbName: {} and tableNamePattern: {}", dbName, tableNamePattern);
        if ( schemaManagerService.getAutoCreateSchema() )
        {
            KsDef ksDef = schemaManagerService.getKeyspaceForDatabaseName(dbName);
            if ( ksDef != null )
            {
                log.debug("Checking for new column families to add");
                schemaManagerService.createNewColumnFamilyTables(ksDef);
            }
        }

        List<TBase> tables = metaStorePersister.find(new Table(), dbName);
        List<String> results = new ArrayList<String>(tables.size());
        for (TBase tBase : tables)
        {
            Table table = (Table)tBase;
            if ( StringUtils.isEmpty(tableNamePattern) || table.getTableName().matches(tableNamePattern))
                results.add(table.getTableName());
        }
        return results;
    }

    public List<String> getAllTables(String databaseName) throws MetaException
    {
        log.debug("in getAllTables");
        return getTables(databaseName, StringUtils.EMPTY);
    }

    public void alterTable(String databaseName, String oldTableName, Table table)
        throws InvalidObjectException, MetaException
    {
        if ( log.isDebugEnabled() )
            log.debug("Altering oldTableName {} on datbase: {} new Table: {}",
                    new Object[]{oldTableName, databaseName, table.getTableName()});

        if ( oldTableName.equalsIgnoreCase(table.getTableName()) )
        {
            createTable(table);
        }
        else
        {
            List<TBase> removeable = updateTableComponents(databaseName, null, oldTableName, table);
            //dropTable(databaseName, oldTableName);
            Table oldTable = new Table();
            oldTable.setDbName(databaseName);
            oldTable.setTableName(oldTableName);
            metaStorePersister.remove(oldTable, databaseName);
            if ( removeable != null && !removeable.isEmpty() )
                metaStorePersister.removeAll(removeable, databaseName);
        }

    }

    private List<TBase> updateTableComponents(String oldDatabaseName, Database database, String oldTableName, Table table)
        throws InvalidObjectException, MetaException
    {

        createTable(table);
        List<Partition> parts = getPartitions(oldDatabaseName, oldTableName, -1);
        List<TBase> toRemove = new ArrayList<TBase>();
        for (Partition partition : parts)
        {
            toRemove.add(partition.deepCopy());
            if ( database != null )
                partition.setDbName(database.getName());
            partition.setTableName(table.getTableName());
            String loc = partition.getSd().getLocation();
            if ( loc.contains(oldTableName))
            {
                partition.getSd().setLocation(loc.replace(oldTableName, table.getTableName()));
            }
            addPartition(partition);
        }
        // getIndexes
        List<Index> indexes = getIndexes(oldDatabaseName, oldTableName, -1);
        for (Index index : indexes)
        {
            toRemove.add(index.deepCopy());
            if ( database != null )
                index.setDbName(database.getName());
            index.setOrigTableName(table.getTableName());
            addIndex(index);
        }

        return toRemove;
    }

    public boolean dropTable(String databaseName, String tableName) throws MetaException
    {
        log.debug("in dropTable with databaseName: {} and tableName: {}", databaseName, tableName);
        Table table = new Table();
        table.setDbName(databaseName);
        table.setTableName(tableName);
        List<TBase> removeables = new ArrayList<TBase>();
        List<Partition> partitions = getPartitions(databaseName, tableName, -1);
        if ( partitions != null && !partitions.isEmpty() )
            removeables.addAll(partitions);
        List<Index> indexes = getIndexes(databaseName, tableName, -1);
        if ( indexes != null && !indexes.isEmpty() )
            removeables.addAll(indexes);
        metaStorePersister.remove(table, databaseName);
        if ( !removeables.isEmpty() )
            metaStorePersister.removeAll(removeables, databaseName);
        return true;
    }

    public boolean addIndex(Index index) throws InvalidObjectException,
            MetaException
    {
        if ( index.getParameters() != null )
        {
            Set<Entry<String, String>> entrySet = index.getParameters().entrySet();
            for (Entry<String, String> entry : entrySet )
            {
                if ( entry.getValue() == null )
                    entrySet.remove(entry);
            }
        }
        metaStorePersister.save(index.metaDataMap, index, index.getDbName());
        return false;
    }

    public Index getIndex(String databaseName, String tableName, String indexName)
            throws MetaException
    {
        if ( log.isDebugEnabled() )
            log.debug("in getIndex with databaseName: {}, tableName: {} indexName: {}",
                    new String[]{databaseName, tableName, indexName});
        Index index = new Index();
        index.setDbName(databaseName);
        index.setIndexName(indexName);
        index.setOrigTableName(tableName);
        try {
            metaStorePersister.load(index, databaseName);
        } catch (NotFoundException nfe) {
            throw new MetaException("Index: " + indexName + " did not exist for table: " + tableName + " in database: " + databaseName );
        }
        return index;
    }

    public List<Index> getIndexes(String databaseName, String originalTableName, int max)
            throws MetaException
    {
        List results = metaStorePersister.find(new Index(), databaseName, originalTableName, max);
        return (List<Index>)results;
    }

    public List<String> listIndexNames(String databaseName, String originalTableName, short max)
            throws MetaException
    {
        List<Index> indexes = getIndexes(databaseName, originalTableName, max);
        List<String> results = new ArrayList<String>(indexes.size());
        for (Index index : indexes)
        {
            results.add(index.getIndexName());
        }
        return results;
    }

    public void alterIndex(String databaseName, String originalTableName,
            String originalIndexName, Index index)
            throws InvalidObjectException, MetaException
    {
        if ( log.isDebugEnabled() )
            log.debug("Altering index {} on database: {} and table: {} Index: {}",
                    new Object[]{ originalIndexName, databaseName, originalTableName, index});
        addIndex(index);
        dropIndex(databaseName, originalTableName, originalIndexName);
    }

    public boolean dropIndex(String databaseName, String originalTableName, String indexName)
            throws MetaException
    {
        if ( log.isDebugEnabled() )
            log.debug("In dropIndex with databaseName: {} and originalTableName: {} indexName: {}",
                    new Object[]{ databaseName, originalTableName, indexName});
        Index index = new Index();
        index.setDbName(databaseName);
        index.setOrigTableName(originalTableName);
        index.setIndexName(indexName);
        metaStorePersister.remove(index, databaseName);
        return true;
    }

    public boolean addPartition(Partition partition) throws InvalidObjectException,
            MetaException
    {
        log.debug("in addPartition with: {}", partition);
        metaStorePersister.save(partition.metaDataMap, partition, partition.getDbName());
        return true;
    }

    public Partition getPartition(String databaseName, String tableName, List<String> partitions)
            throws MetaException, NoSuchObjectException
    {
        log.debug("in getPartition databaseName: {} tableName: {} partitions: {}",
                new Object[]{databaseName, tableName, partitions});
        Partition partition = new Partition();
        partition.setDbName(databaseName);
        partition.setTableName(tableName);
        partition.setValues(partitions);
        try {
            metaStorePersister.load(partition, databaseName);
        } catch (NotFoundException e) {
            throw new NoSuchObjectException("Could not find partition for: " + partitions + " on table: " + tableName + " in database: " + databaseName);
        }
        return partition;
    }

    public List<Partition> getPartitions(String databaseName, String tableName, int max)
            throws MetaException
    {
        log.debug("in getPartitions: databaseName: {} tableName: {} max: {}",
                new Object[]{databaseName, tableName, max});

        List results = metaStorePersister.find(new Partition(), databaseName, tableName, max);
        log.debug("Found partitions: {}", results);
        return (List<Partition>)results;
    }

    public List<String> listPartitionNames(String databaseName, String tableName, short max)
            throws MetaException
    {
        log.debug("in listPartitionNames: databaseName: {} tableName: {} max: {}",
                new Object[]{databaseName, tableName, max});
        List<Partition> partitions = getPartitions(databaseName, tableName, max);
        List<String> results = new ArrayList<String>(partitions.size());
        if ( partitions == null )
            return results;
        for (Partition partition : partitions)
        {
            results.add(partition.getSd().getLocation());
        }
        return results;
    }

    public void alterPartition(String databaseName, String tableName, Partition partition)
            throws InvalidObjectException, MetaException
    {
        if ( log.isDebugEnabled() )
            log.debug("Altering partiion for table {} on database: {} Partition: {}",
                    new Object[]{tableName, databaseName, partition});
        Partition oldPartition;
        try
        {
            oldPartition = getPartition(databaseName, tableName, partition.getValues());
        }
        catch (NoSuchObjectException nse)
        {
            throw new InvalidObjectException(nse.getMessage());
        }
        addPartition(partition);
    }

    public boolean dropPartition(String databaseName, String tableName, List<String> partitions)
            throws MetaException
    {
        Partition partition = new Partition();
        partition.setDbName(databaseName);
        partition.setTableName(tableName);
        partition.setValues(partitions);
        if ( log.isDebugEnabled() )
            log.debug("Dropping partition: {}", partition);
        metaStorePersister.remove(partition, databaseName);
        return true;
    }

    public boolean addRole(String roleName, String ownerName)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        Role role = new Role();
        role.setOwnerName(ownerName);
        role.setRoleName(roleName);
        metaStorePersister.save(role.metaDataMap, role, CassandraClientHolder.META_DB_ROW_KEY);
        return true;
    }

    public Role getRole(String roleName) throws NoSuchObjectException
    {
        Role role = new Role();
        role.setRoleName(roleName);
        try {
            metaStorePersister.load(role, CassandraClientHolder.META_DB_ROW_KEY);
        } catch (NotFoundException nfe) {
            throw new NoSuchObjectException("could not find role: " + roleName);
        }
        return role;
    }

    public boolean createType(Type type)
    {
        metaStorePersister.save(type.metaDataMap, type, CassandraClientHolder.META_DB_ROW_KEY);
        return true;
    }

    public Type getType(String type)
    {
        Type t = new Type();
        t.setName(type);
        try {
            metaStorePersister.load(t, CassandraClientHolder.META_DB_ROW_KEY);
        } catch (NotFoundException e) {
            return null;
        }
        return t;
    }

    public boolean dropType(String type)
    {
        Type t = new Type();
        t.setName(type);
        metaStorePersister.remove(t, CassandraClientHolder.META_DB_ROW_KEY);
        return true;
    }


    public boolean commitTransaction()
    {
        // FIXME default to true for now
        return true;
    }

    @Override
    public PrincipalPrivilegeSet getColumnPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, String arg4,
            List<String> arg5) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getDBPrivilegeSet(String arg0, String arg1,
            List<String> arg2) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getPartitionPrivilegeSet(String arg0,
            String arg1, String arg2, String arg3, List<String> arg4)
            throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Partition getPartitionWithAuth(String databaseName, String tableName,
            List<String> partVals, String userName, List<String> groupNames)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        log.debug("in getPartitionWithAuth: databaseName: {} tableName: {} userName: {} groupNames: {}",
                new Object[]{databaseName, tableName, userName, groupNames});
        return getPartition(databaseName, tableName, partVals);
    }

    public List<Partition> getPartitionsWithAuth(String databaseName, String tableName,
            short maxParts, String userName, List<String> groupNames) throws MetaException,
            NoSuchObjectException, InvalidObjectException
    {
        log.debug("in getPartitionsWithAuth: databaseName: {} tableName: {} maxParts: {} userName: {}",
                new Object[]{databaseName, tableName, maxParts, userName});

        List<Partition> partitions = getPartitions(databaseName, tableName, maxParts);

        return partitions;
    }

    @Override
    public List<Partition> getPartitionsByFilter(String databaseName, String tableName,
            String filter, short maxPartitions) throws MetaException,
            NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PrincipalPrivilegeSet getTablePrivilegeSet(String arg0, String arg1,
            String arg2, List<String> arg3) throws InvalidObjectException,
            MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public PrincipalPrivilegeSet getUserPrivilegeSet(String arg0,
            List<String> arg1) throws InvalidObjectException, MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean grantPrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean grantRole(Role arg0, String arg1, PrincipalType arg2,
            String arg3, PrincipalType arg4, boolean arg5)
            throws MetaException, NoSuchObjectException, InvalidObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public List<MTablePrivilege> listAllTableGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listPartitionNamesByFilter(String arg0, String arg1,
            String arg2, short arg3) throws MetaException
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MDBPrivilege> listPrincipalDBGrants(String arg0,
            PrincipalType arg1, String arg2)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MGlobalPrivilege> listPrincipalGlobalGrants(String arg0,
            PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4, String arg5)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MPartitionPrivilege> listPrincipalPartitionGrants(String arg0,
            PrincipalType arg1, String arg2, String arg3, String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
            String arg0, PrincipalType arg1, String arg2, String arg3,
            String arg4)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listRoleNames()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<MRoleMap> listRoles(String arg0, PrincipalType arg1)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean openTransaction()
    {
        // TODO Auto-generated method stub
        return true;
    }

    @Override
    public boolean removeRole(String arg0) throws MetaException,
            NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean revokePrivileges(PrivilegeBag arg0)
            throws InvalidObjectException, MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean revokeRole(Role arg0, String arg1, PrincipalType arg2)
            throws MetaException, NoSuchObjectException
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void rollbackTransaction()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown()
    {
        // TODO Auto-generated method stub

    }


  /* TODO: Implement these methods */

  @Override
  public List<Table> getTableObjectsByName(String s, List<String> strings) throws MetaException, UnknownDBException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<String> listTableNamesByFilter(String s, String s2, short i) throws MetaException, UnknownDBException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void alterPartition(String s, String s2, List<String> strings, Partition partition) throws InvalidObjectException, MetaException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<Partition> getPartitionsByNames(String s, String s2, List<String> strings) throws MetaException, NoSuchObjectException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Table markPartitionForEvent(String s, String s2, Map<String, String> stringStringMap, PartitionEventType partitionEventType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isPartitionMarkedForEvent(String s, String s2, Map<String, String> stringStringMap, PartitionEventType partitionEventType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<String> listPartitionNamesPs(String s, String s2, List<String> strings, short i) throws MetaException, NoSuchObjectException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String s, String s2, List<String> strings, short i, String s3, List<String> strings2) throws MetaException, InvalidObjectException, NoSuchObjectException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public long cleanupEvents() {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
