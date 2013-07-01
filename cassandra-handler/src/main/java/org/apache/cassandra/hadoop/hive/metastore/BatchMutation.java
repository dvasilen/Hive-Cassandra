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

import org.apache.cassandra.thrift.*;

/**
 * Wrapper class (originally from hector-core) to make dealing with batch_mutate 
 * not quite as horrible.
 */
public class BatchMutation
{
    private final Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap;


    public BatchMutation()
    {
        mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
    }

    private BatchMutation(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap)
    {     
        this.mutationMap = mutationMap;
    }

    /**
     * Add an Column insertion (or update) to the batch mutation request.
     */
    public BatchMutation addInsertion(ByteBuffer key, List<String> columnFamilies,
            Column column)
    {
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn()
                .setColumn(column));
        addMutation(key, columnFamilies, mutation);
        return this;
    }

    /**
     * Add an SuperColumn insertion (or update) to the batch mutation request.
     */
    public BatchMutation addSuperInsertion(ByteBuffer key,
            List<String> columnFamilies, SuperColumn superColumn)
    {
        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(new ColumnOrSuperColumn()
                .setSuper_column(superColumn));
        addMutation(key, columnFamilies, mutation);
        return this;
    }

    /**
     * Add a deletion request to the batch mutation.
     */
    public BatchMutation addDeletion(ByteBuffer key, List<String> columnFamilies,
            Deletion deletion)
    {
        Mutation mutation = new Mutation();
        mutation.setDeletion(deletion);
        addMutation(key, columnFamilies, mutation);
        return this;
    }

    private void addMutation(ByteBuffer key, List<String> columnFamilies,
            Mutation mutation)
    {
        Map<String, List<Mutation>> innerMutationMap = getInnerMutationMap(key);
        for (String columnFamily : columnFamilies)
        {
            if (innerMutationMap.get(columnFamily) == null)
            {
                innerMutationMap.put(columnFamily, Arrays.asList(mutation));
            } else
            {
                List<Mutation> mutations = new ArrayList<Mutation>(
                        innerMutationMap.get(columnFamily));
                mutations.add(mutation);
                innerMutationMap.put(columnFamily, mutations);
            }
        }
        mutationMap.put(key, innerMutationMap);
    }

    private Map<String, List<Mutation>> getInnerMutationMap(ByteBuffer key)
    {
        Map<String, List<Mutation>> innerMutationMap = mutationMap.get(key);
        if (innerMutationMap == null)
        {
            innerMutationMap = new HashMap<String, List<Mutation>>();
        }
        return innerMutationMap;
    }

    Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap()
    {
        return mutationMap;
    }

    /**
     * Makes a shallow copy of the mutation object.
     * 
     * @return
     */
    public BatchMutation makeCopy()
    {
        return new BatchMutation(mutationMap);
    }

    /**
     * Checks whether the mutation object contains any mutations.
     * 
     * @return
     */
    public boolean isEmpty()
    {
        return mutationMap.isEmpty();
    }

}
