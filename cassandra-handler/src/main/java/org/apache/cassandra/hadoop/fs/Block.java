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

import java.util.UUID;

import org.apache.log4j.Logger;

public class Block
{
    private static Logger logger = Logger.getLogger(Block.class);
    
    public final UUID id;
    public final long length;
    public final long offset;
    public final SubBlock[] subBlocks;
    
    public Block(UUID id, long offset, long length, SubBlock[] subBlocks)
    {
        this.id     = id;
        this.offset = offset;
        this.length = length;
        this.subBlocks = subBlocks;
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("Block[" + id + ", " + offset + ", "+ length + "\n");

      if (subBlocks != null) 
      {
           for (SubBlock sblock : subBlocks) {
               sb.append("    " + sblock.toString());
           }
      }

      return sb.toString();
    }

}
