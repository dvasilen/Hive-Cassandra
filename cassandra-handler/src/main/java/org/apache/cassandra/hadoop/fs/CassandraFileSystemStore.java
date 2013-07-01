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
package org.apache.cassandra.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

/**
 * A facility for storing and retrieving {@link org.apache.cassandra.hadoop.fs.INode}s and {@link org.apache.cassandra.hadoop.fs.Block}s.
 */
public interface CassandraFileSystemStore
{

    void initialize(URI uri, Configuration conf) throws IOException;

    String getVersion() throws IOException;

    void storeINode(Path path, INode inode) throws IOException;

    /**
     * Stores a SubBlock.
     * 
     * @param currentBlockUUID parent UUID used as row key for the subBlock row.
     * @param subBlock sub Block to be written 
     * @param data content of the subBLock
     * @throws java.io.IOException if an error occur
     */
    void storeSubBlock(UUID currentBlockUUID, SubBlock subBlock, ByteBuffer data) throws IOException;

    INode retrieveINode(Path path) throws IOException;

    InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException;
    
    /**
     * Retrieves an inputStream associated to the SubBlock content.
     * 
     * @param block parent block of the subBlock to retrieve
     * @param subBlock the subBLock to retrieve
     * @param byteRangeStart the offset where to stream the data from.
     * @return an inputStream to retrieve the content of the subBlock
     * @throws java.io.IOException if an error occurs
     */
    InputStream retrieveSubBlock(Block block, SubBlock subBlock, long byteRangeStart) throws IOException;

    /**
     * Delete an inode from the persistent layer.
     * 
     * @param path inode path
     * @throws java.io.IOException if an error occurs
     */
    void deleteINode(Path path) throws IOException;

    /**
     * Deletes all subBlocks for the inode.
     * @param inode parent inode of these subBlocks
     * @throws java.io.IOException if an error occurs
     */
    void deleteSubBlocks(INode inode) throws IOException;

    Set<Path> listSubPaths(Path path) throws IOException;

    Set<Path> listDeepSubPaths(Path path) throws IOException;

    BlockLocation[] getBlockLocation(List<Block> usedBlocks, long start, long len) throws IOException;
}