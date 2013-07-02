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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.hadoop.fs.permission.FsPermission;

public class INode
{
    enum FileType
    {
        DIRECTORY, FILE
    }

    public static final FileType[] FILE_TYPES      = { FileType.DIRECTORY, FileType.FILE };

    private FileType               fileType;
    public final String            user;
    public final String            group;
    public final FsPermission      perms;
    private Block[]                blocks;
    public long mtime = 0;

    public INode(String user, String group, FsPermission perms, FileType fileType, Block[] blocks)
    {
        
        this.user = user;
        this.group = group;
        this.perms = perms;
        this.fileType = fileType;
        
        if (isDirectory() && blocks != null)
        {
            throw new IllegalArgumentException("A directory cannot contain blocks.");
        }
        this.blocks = blocks;
    }

    public INode(String user, String group, FsPermission perms, FileType fileType, Block[] blocks, long mtime)
    {
        this(user, group, perms, fileType, blocks);
        this.mtime = mtime;
    }

    public Block[] getBlocks()
    {
        return blocks;
    }

    public FileType getFileType()
    {
        return fileType;
    }

    public boolean isDirectory()
    {
        return fileType == FileType.DIRECTORY;
    }

    public boolean isFile()
    {
        return fileType == FileType.FILE;
    }

    public ByteBuffer serialize() throws IOException
    {
    	// Write INode header
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(user.getBytes().length);  out.writeBytes(user);
        out.writeInt(group.getBytes().length); out.writeBytes(group);
        out.writeShort(perms.toShort());
        out.writeByte(fileType.ordinal());
        if (isFile())
        {  
            // Write blocks
            out.writeInt(blocks.length);
            for (int i = 0; i < blocks.length; i++)
            {
                out.writeLong(blocks[i].id.getMostSignificantBits());
                out.writeLong(blocks[i].id.getLeastSignificantBits());

                out.writeLong(blocks[i].offset);
                out.writeLong(blocks[i].length);
                
                // Write SubBlocks for this block
                out.writeInt(blocks[i].subBlocks.length);
                for (SubBlock subb : blocks[i].subBlocks)
                {
                    out.writeLong(subb.id.getMostSignificantBits());
                    out.writeLong(subb.id.getLeastSignificantBits());
                    out.writeLong(subb.offset);
                    out.writeLong(subb.length);
                }
            }
        }
        out.close();
        return ByteBuffer.wrap(bytes.toByteArray());
    }

    public static INode deserialize(InputStream in, long ts) throws IOException
    {
        if (in == null)
        {
            return null;
        }
        DataInputStream dataIn = new DataInputStream(in);
        int ulen = dataIn.readInt();
        byte[] ubuf = new byte[ulen];
        dataIn.readFully(ubuf);
        
        int glen = dataIn.readInt();
        byte[] gbuf = new byte[glen];
        dataIn.readFully(gbuf);
        
        FsPermission perms = new FsPermission(dataIn.readShort());
        
        FileType fileType = INode.FILE_TYPES[dataIn.readByte()];
        switch (fileType)
        {
        case DIRECTORY:
            in.close();
            return new INode(new String(ubuf), new String(gbuf), perms, fileType, null, ts);
        case FILE:
            int numBlocks = dataIn.readInt();
            Block[] blocks = new Block[numBlocks];
            for (int i = 0; i < numBlocks; i++)
            {
                long mostSigBits = dataIn.readLong();
                long leastSigBits = dataIn.readLong();
                long offset = dataIn.readLong();
                long length = dataIn.readLong();
                
                // Deserialize SubBlocks for this block
                int numSubBlocks = dataIn.readInt();
                SubBlock[] subBlocks = new SubBlock[numSubBlocks];
                for (int j = 0; j < numSubBlocks; j++)
                {
                    long subMostSigBits = dataIn.readLong();
                    long subLeastSigBits = dataIn.readLong();
                    long subOffset = dataIn.readLong();
                    long subLength = dataIn.readLong();
                    subBlocks[j] = new SubBlock(new UUID(subMostSigBits,subLeastSigBits), subOffset, subLength);
                }
                
                blocks[i] = new Block(new UUID(mostSigBits,leastSigBits), offset, length, subBlocks);
            }
            in.close();
            return new INode(new String(ubuf), new String(gbuf), perms, fileType, blocks, ts);
        default:
            throw new IllegalArgumentException("Cannot deserialize inode.");
        }
    }

}
