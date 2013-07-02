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
package org.apache.cassandra.hadoop.cafs.core;


import junit.framework.Assert;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;

/**
 * Test {@link INode} functionality.
 *
 */
public class INodeTest {

	/**
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testSerializationForFileInode() throws Exception {
		// Create sample data.
		Sample1.init();
		
		// Serialize and deserialize the sample
		InputStream in = new ByteArrayInputStream(Sample1.inode.serialize().array());
		
		// Assert with customized assertion method.
		assertINodeEqual(Sample1.inode, INode.deserialize(in, 0));
		
		// Just make sure we are testing the right sample.
		Assert.assertEquals(2, Sample1.inode.getBlocks().length);
	}
	
	@Test
	public void testSerializationForDirectoryInode() throws Exception {
		// Create sample data.
		Sample2.init();
		
		// Serialize and deserialize the sample
		InputStream in = new ByteArrayInputStream(Sample2.inode.serialize().array());
		
		// Assert with customized assertion method.
		assertINodeEqual(Sample2.inode, INode.deserialize(in, 0));
	}
	
	/**
	 * Verifies equality condition for inodes. 
	 */
	private void assertINodeEqual(INode inode1, INode inode2) {
		Assert.assertEquals(inode1.group, inode2.group);
		Assert.assertEquals(inode1.mtime, inode2.mtime);
		Assert.assertEquals(inode1.user, inode2.user);
		Assert.assertEquals(inode1.perms, inode2.perms);
		Assert.assertEquals(inode1.isDirectory(), inode2.isDirectory());
		Assert.assertEquals(inode1.isFile(), inode2.isFile());
		
		// If inodes are directories, it will not have blocks.
		if (inode1.isDirectory()) return;
		
		Assert.assertEquals(inode1.getBlocks().length, inode2.getBlocks().length);
		
		// Iterate through the blocks based inode1
		for (int i = 0 ; i < inode1.getBlocks().length; i++) {
			assertBlockEquals(inode1.getBlocks()[0], inode2.getBlocks()[0]);
		}
		
		// Iterate through the blocks based inode1
		for (int i = 0 ; i < inode2.getBlocks().length; i++) {
			assertBlockEquals(inode2.getBlocks()[0], inode1.getBlocks()[0]);
		}
	}
	
	/**
	 * Verifies equality condition for blocks. 
	 */
	private void assertBlockEquals(Block block1, Block block2) {
		Assert.assertEquals(block1.id.toString(), block2.id.toString());
		Assert.assertEquals(block1.length, block2.length);
		Assert.assertEquals(block1.offset, block2.offset);
		Assert.assertEquals(block1.subBlocks.length, block2.subBlocks.length);
		
		for(int i = 0 ; i < block1.subBlocks.length; i++) {
			assertSubBlockEquals(block1.subBlocks[0], block2.subBlocks[0]);
		}
		
		for(int i = 0 ; i < block2.subBlocks.length; i++) {
			assertSubBlockEquals(block2.subBlocks[0], block1.subBlocks[0]);
		}
	}

	/**
	 * Verifies equality condition for SubBlocks.
	 */
	private void assertSubBlockEquals(SubBlock subBlock1, SubBlock subBlock2) {
		Assert.assertEquals(subBlock1.id.toString(), subBlock2.id.toString());
		Assert.assertEquals(subBlock1.length, subBlock2.length);
		Assert.assertEquals(subBlock1.offset, subBlock2.offset);
	}

	/**
	 * 1 INode
	 * 2 Blocks
	 * 2 SubBlocks per Block
	 * 
	 * @author patricioe (Patricio Echague - patricioe@gmail.com)
	 *
	 */
	private static class Sample1 {
		
		public static Block[] blocks = new Block[2];
		
		public static INode inode;
		
		public static void init() {
			SubBlock[] subBlocks = new SubBlock[2];
			subBlocks[0] = new SubBlock(UUID.randomUUID(), 0, 128);
			subBlocks[1] = new SubBlock(UUID.randomUUID(), 128, 128);
			
			blocks[0] = new Block(UUID.randomUUID(), 0, 256, subBlocks);
			
			subBlocks = new SubBlock[2];
			subBlocks[0] = new SubBlock(UUID.randomUUID(), 0, 128);
			subBlocks[1] = new SubBlock(UUID.randomUUID(), 128, 128);
			
			blocks[1] = new Block(UUID.randomUUID(), 0, 256, subBlocks);
			
			inode = new INode("user", "group", FsPermission.getDefault(), INode.FileType.FILE, blocks);
		}
	}
	
	/**
	 * 1 INode
	 * 0 Block
	 * 0 SubBlock per Block
	 *
	 */
	private static class Sample2 {
		
		public static INode inode;
		
		public static void init() {
			
			inode = new INode("user", "group", FsPermission.getDefault(), INode.FileType.DIRECTORY, null);
		}
	}

}
