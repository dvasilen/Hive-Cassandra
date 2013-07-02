package org.apache.cassandra.hadoop.cafs.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface CaFileSystemStore {


  void initialize(URI uri, Configuration conf) throws IOException;

  String getVersion() throws IOException;

  void storeINode(Path path, INode inode) throws IOException;

  INode retrieveINode(Path path) throws IOException;

  /**
   * Store a subblock
   *
   * @param currentBlockUUID parent UUID used as row key for subblock
   * @param subBlock         subblock to be written
   * @param data             content of the subblock
   */
  void storeSubBlock(UUID currentBlockUUID, SubBlock subBlock, ByteBuffer data) throws IOException;

  /**
   * Retrieve a subblock
   *
   * @param block          parent block of the subblock to be retrieved
   * @param subBlock       the subblock to be retrieved
   * @param byteRangeStart the offset where to stream the data
   * @return an InputStream
   * @throws java.io.IOException
   */
  InputStream retrieveSubBlock(Block block, SubBlock subBlock, long byteRangeStart) throws IOException;

  InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException;

  void deleteINode(Path path) throws IOException;

  /**
   * delete all subblocks for the inode
   *
   * @param inode
   * @throws java.io.IOException
   */
  void deleteSubBlocks(INode inode) throws IOException;

  Set<Path> listSubPaths(Path path) throws IOException;

  Set<Path> listDeepSubPaths(Path path) throws IOException;

  BlockLocation[] getBlockLocation(List<Block> usedBlocks, long start, long len) throws IOException;

}
