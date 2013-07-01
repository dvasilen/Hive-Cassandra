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

import java.nio.ByteBuffer;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.*;
import org.apache.thrift.meta_data.FieldMetaData;

/**
 * Generically persist and load the Hive Meta Store model classes
 * 
 *
 */
public class MetaStorePersister
{
    private static final String COL_NAME_SEP = "::";

    private static final Logger log = LoggerFactory.getLogger(MetaStorePersister.class);
    
    private TSerializer serializer;
    private TDeserializer deserializer;
    private CassandraClientHolder cassandraClientHolder;
    
    public MetaStorePersister(Configuration conf) 
    {
        cassandraClientHolder = new CassandraClientHolder(conf);
        cassandraClientHolder.applyKeyspace();
    }

    @SuppressWarnings("unchecked")
    public void save(Map<? extends TFieldIdEnum, FieldMetaData> metaData,
            TBase base, String databaseName) throws CassandraHiveMetaStoreException
    {
        databaseName = databaseName.toLowerCase();
        // FIXME turns out metaData is not needed anymore. Remove from sig.
        // TODO need to add ID field to column name lookup to avoid overwrites
        if ( log.isDebugEnabled() )
            log.debug("in save with class: {} dbname: {}", base, databaseName);
        serializer = new TSerializer();        
        BatchMutation batchMutation = new BatchMutation();

        try
        {
            batchMutation.addInsertion(ByteBufferUtil.bytes(databaseName), 
                    Arrays.asList(cassandraClientHolder.getColumnFamily()), 
                    new Column()
            .setName(ByteBufferUtil.bytes(buildEntityColumnName(base)))
            .setValue(ByteBuffer.wrap(serializer.serialize(base)))
            .setTimestamp(System.currentTimeMillis()));
                       
            cassandraClientHolder.getClient().batch_mutate(batchMutation.getMutationMap(),
                    cassandraClientHolder.getWriteCl());
        } 
        catch (Exception e)
        {
            // TODO add exception handling wrapper
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }

    }
    
    @SuppressWarnings("unchecked")
    public TBase load(TBase base, String databaseName) 
        throws CassandraHiveMetaStoreException, NotFoundException
    {
        databaseName = databaseName.toLowerCase();
        if ( log.isDebugEnabled() )
            log.debug("in load with class: {} dbname: {}", base.getClass().getName(), databaseName);
        deserializer = new TDeserializer();        
        try 
        {
            ColumnPath columnPath = new ColumnPath(cassandraClientHolder.getColumnFamily());
            columnPath.setColumn(ByteBufferUtil.bytes(buildEntityColumnName(base)));
            ColumnOrSuperColumn cosc = cassandraClientHolder.getClient()
                .get(ByteBufferUtil.bytes(databaseName), columnPath, 
                        cassandraClientHolder.getReadCl());
            deserializer.deserialize(base, cosc.getColumn().getValue());
        } 
        catch (NotFoundException nfe)
        {
            throw nfe;
        }
        catch (Exception e) 
        {
            // TODO same exception handling wrapper as above
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }                
        return base;
    }
    
    public List<TBase> find(TBase base, String databaseName) 
    throws CassandraHiveMetaStoreException
    {
        return find(base, databaseName, null, 100);
    }
    
    @SuppressWarnings("unchecked")
    public List<TBase> find(TBase base, String databaseName, String prefix, int count) 
        throws CassandraHiveMetaStoreException
    {
        databaseName = databaseName.toLowerCase();
        if ( log.isDebugEnabled() )
            log.debug("in find with class: {} dbname: {} prefix: {} and count: {}", 
                    new Object[]{base.getClass().getName(), databaseName, prefix, count});
        if ( count < 0 ) {
            // TODO make this a config parameter.
            count = 100;
        }
        deserializer = new TDeserializer();
        
        List<TBase> resultList;
        try 
        {
            SlicePredicate predicate = new SlicePredicate();
            predicate.setSlice_range(buildEntitySlicePrefix(base, prefix, count));            
            List<ColumnOrSuperColumn> cols = cassandraClientHolder.getClient().get_slice(ByteBufferUtil.bytes(databaseName), 
                    new ColumnParent(cassandraClientHolder.getColumnFamily()), 
                    predicate, 
                    cassandraClientHolder.getReadCl());
            resultList = new ArrayList<TBase>(cols.size());
            for (ColumnOrSuperColumn cosc : cols)
            {
                TBase other = base.getClass().newInstance();
                deserializer.deserialize(other, cosc.getColumn().getValue());                
                resultList.add(other);
            }             
        } 
        catch (Exception e) 
        {
            // TODO same exception handling wrapper as above
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }                
        return resultList;
    }
    
    @SuppressWarnings("unchecked")
    public void remove(TBase base, String databaseName) 
    {
        removeAll(Arrays.asList(base), databaseName);    
    }
    
    @SuppressWarnings("unchecked")
    public void removeAll(List<TBase> bases, String databaseName) 
    {
        databaseName = databaseName.toLowerCase();
        serializer = new TSerializer();        
        BatchMutation batchMutation = new BatchMutation();     
        try
        {    
            Deletion deletion = new Deletion().setTimestamp(System.currentTimeMillis());
            SlicePredicate predicate = new SlicePredicate();
            
            for (TBase tBase : bases)
            {
                if ( log.isDebugEnabled() )
                    log.debug("in remove with class: {} dbname: {}", tBase, databaseName);
                predicate.addToColumn_names(ByteBufferUtil.bytes(buildEntityColumnName(tBase)));            
            }
            deletion.setPredicate(predicate);
            batchMutation.addDeletion(ByteBufferUtil.bytes(databaseName), 
                    Arrays.asList(cassandraClientHolder.getColumnFamily()), deletion);            
            cassandraClientHolder.getClient().batch_mutate(batchMutation.getMutationMap(),
                    cassandraClientHolder.getWriteCl());
        } 
        catch (Exception e)
        {            
            throw new CassandraHiveMetaStoreException(e.getMessage(), e);
        }
    
    }
    
    private String buildEntityColumnName(TBase base) {
        StringBuilder colName = new StringBuilder(96);
        colName.append(base.getClass().getName()).append(COL_NAME_SEP);
        if ( base instanceof Database )
        {
            colName.append(((Database)base).getName().toLowerCase());
        } else if ( base instanceof Table ) 
        {
            colName.append(((Table)base).getTableName().toLowerCase());
        } else if ( base instanceof Index ) 
        {
            colName.append(((Index)base).getOrigTableName())
            .append(COL_NAME_SEP)
            .append(((Index)base).getIndexName());
        } else if ( base instanceof Partition ) 
        {
            colName.append(((Partition)base).getTableName().toLowerCase());
            for( String value : ((Partition)base).getValues()) 
            {
                colName.append(COL_NAME_SEP).append(value);
            }
        } else if ( base instanceof Type )
        {
            colName.append(((Type)base).getName());            
        } else if ( base instanceof Role)
        {
            colName.append(((Role)base).getRoleName());
        } 
        if ( log.isDebugEnabled() )
            log.debug("Constructed columnName: {}", colName);
        return colName.toString();
    }
    
    /**
     * Construct a basic slice op, since we do most of our filtering client side
     *     
     * @param base the object we use for the column name
     * @param prefix the prefix to prepend. Can be null.
     * @param count the number of items to which we restrict this predicate
     */
    private SliceRange buildEntitySlicePrefix(TBase base, String prefix, int count) {
        StringBuilder colName = new StringBuilder(96);
        colName.append(base.getClass().getName()).append(COL_NAME_SEP);
        if ( prefix != null && !prefix.isEmpty() ) 
            colName.append(prefix);
        SliceRange sliceRange = new SliceRange(ByteBufferUtil.bytes(colName.toString()), 
                ByteBufferUtil.bytes(colName.append("|").toString()), false, count);
        if ( log.isDebugEnabled() )
            log.debug("Constructed columnName for slice: {}", colName);
        return sliceRange;
    }
}
