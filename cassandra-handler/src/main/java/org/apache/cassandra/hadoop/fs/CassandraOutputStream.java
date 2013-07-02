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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class CassandraOutputStream extends OutputStream {

  private Configuration conf;

  private int bufferSize;

  private CassandraFileSystemStore store;

  private Path path;

  private long blockSize;

  private long subBlockSize;

  private ByteBuffer backupStream;

  private boolean closed;

  private int pos = 0;

  private long filePos = 0;

  private long bytesWrittenToBlock = 0;

  private long bytesWrittenToSubBlock = 0;

  private byte[] outBuf;

  private List<Block> blocks = new ArrayList<Block>();

  private Block nextBlock;

  private List<SubBlock> subBlocks = new ArrayList<SubBlock>();

  private SubBlock nextSubBlock;

  private final Progressable progress;

  private FsPermission perms;

  /**
   * Holds the current block UUID to be written.
   * Since the subBLock is written first, we need to know the Block UUID ahead of time.
   */
  private UUID currentBlockUUID;

  public CassandraOutputStream(Configuration conf, CassandraFileSystemStore store, Path path, FsPermission perms,
                               long blockSize, long subBlockSize, Progressable progress, int buffersize) throws IOException {
    this.conf = conf;
    this.store = store;
    this.path = path;
    this.blockSize = blockSize;
    this.subBlockSize = subBlockSize;
    this.backupStream = ByteBuffer.allocateDirect((int) subBlockSize);
    this.bufferSize = buffersize;
    this.progress = progress;
    this.outBuf = new byte[bufferSize];
    this.perms = perms;
    this.currentBlockUUID = generateTimeUUID();

    // Integrity check.
    if (blockSize < subBlockSize) {
      throw new IllegalArgumentException(
              String.format("blockSize{%d} cannot be smaller than SubBlockSize{%d}", blockSize, subBlockSize));
    }
  }

  public long getPos() throws IOException {
    return filePos;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if ((bytesWrittenToBlock + pos == blockSize)
            || (bytesWrittenToSubBlock + pos == subBlockSize)
            || (pos >= bufferSize)) {
      flush();
    }
    outBuf[pos++] = (byte) b;
    filePos++;
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    while (len > 0) {
      int remaining = bufferSize - pos;
      int toWrite = Math.min(remaining, len);
      toWrite = Math.min(toWrite, (int) subBlockSize);

      System.arraycopy(b, off, outBuf, pos, toWrite);
      pos += toWrite;
      off += toWrite;
      len -= toWrite;
      filePos += toWrite;

      if (overFlowsBlockSize() || overFlowsSubBlockSize() || reachedLocalBuffer()) {
        flush();
      }
    }
  }

  /**
   * @return TRUE if the subBlock size is overflowed due to probably last write (pos variable).
   */
  private boolean overFlowsSubBlockSize() {
    return bytesWrittenToSubBlock + pos >= subBlockSize;
  }


  /**
   * @return TRUE if the local buffer size has been reached.
   */
  private boolean reachedLocalBuffer() {
    return pos == bufferSize;
  }

  /**
   * @return TRUE if the block size is overflowed due to probably last write (pos variable).
   */
  private boolean overFlowsBlockSize() {
    return bytesWrittenToBlock + pos >= blockSize;
  }

  /**
   * @return TRUE if the block limit has been reached.
   */
  private boolean reachedBlockSize() {
    return bytesWrittenToBlock == blockSize;
  }

  /**
   * @return TRUE if the subBlock limit has been reached.
   */
  private boolean reachedSubBlockSize() {
    return bytesWrittenToSubBlock == subBlockSize;
  }

  @Override
  public synchronized void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (overFlowsBlockSize() || overFlowsSubBlockSize()) {
      flushData((int) (subBlockSize - bytesWrittenToSubBlock));
    }

    if (reachedSubBlockSize()) {
      endSubBlock();
    }

    if (reachedBlockSize()) {
      // If there is a partial subBlock
      if (bytesWrittenToSubBlock != 0) {
        // means that we have some data in the subBlock to be flushed.
        // we also need to truncate the current subBlock as the Block is done.
        endSubBlock();
      } // NO ELSE. endSubBlock was called before. Nothing to worry about.

      endBlock();
    }

    // Flush remaining data from the last write.
    flushData(pos);
  }


  private synchronized void flushData(int maxPos) throws IOException {
    int workingPos = Math.min(pos, maxPos);

    if (workingPos > 0) {
      // To the local block backup, write just the bytes
      backupStream.put(outBuf, 0, workingPos);

      // Track position
      bytesWrittenToBlock += workingPos;
      bytesWrittenToSubBlock += workingPos;

      System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
      pos -= workingPos;
    }
  }

  private synchronized void endBlock() throws IOException {
    // Send it to Cassandra
    if (progress != null)
      progress.progress();

    nextBlockOutputStream();

    //store.storeBlock(nextBlock, backupStream);
    //internalClose();

    bytesWrittenToBlock = 0;
  }

  /**
   * - flushes the current buffer into the DB,
   * - reset the backupStream, and
   * - reset subBlock counter to 0 for the next subBlock.
   */
  private synchronized void endSubBlock() throws IOException {

    if (progress != null)
      progress.progress();

    nextSubBlockOutputStream();

    backupStream.limit(backupStream.position());
    backupStream.rewind();

    store.storeSubBlock(currentBlockUUID, nextSubBlock, backupStream);

    // Get the stream ready for next subBlock
    backupStream.limit(backupStream.capacity());
    backupStream.rewind();

    // Reset counter for subBlock as this subBlock is full.
    bytesWrittenToSubBlock = 0;
  }

  private synchronized void nextSubBlockOutputStream() {
    // SubBlock  offset ==> bytesWrittenToBlock - bytesWrittenToSubBlock - pos
    nextSubBlock = new SubBlock(generateTimeUUID(),
            bytesWrittenToBlock - bytesWrittenToSubBlock - pos, bytesWrittenToSubBlock);

    subBlocks.add(nextSubBlock);

    // Reset counter for subBlock as this subBlock is full.
    bytesWrittenToSubBlock = 0;
  }

  private synchronized void nextBlockOutputStream() throws IOException {
    nextBlock = new Block(currentBlockUUID,
            filePos - bytesWrittenToBlock - pos, bytesWrittenToBlock,
            subBlocks.toArray(new SubBlock[]{}));
    blocks.add(nextBlock);
    // Clean up the sub blocks collection for the next block.
    subBlocks.clear();
    bytesWrittenToBlock = 0;

    // Generate the next UUID for the upcoming block so that subBlocks can reference to it.
    currentBlockUUID = generateTimeUUID();
  }

  private UUID generateTimeUUID() {
    return UUIDGen.getTimeUUID();
  }

  private synchronized void internalClose() throws IOException {
    INode inode = new INode(
            System.getProperty("user.name", "none"),
            System.getProperty("user.name", "none"),
            perms,
            INode.FileType.FILE,
            blocks.toArray(new Block[]{}));

    store.storeINode(path, inode);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    flush();
    if (filePos == 0 || bytesWrittenToBlock != 0) {
      if (bytesWrittenToSubBlock != 0) {
        endSubBlock();
      }
      endBlock();
    }

    // Save the INode to the DB after ending the subBlocks and Blocks.
    internalClose();
    super.close();

    closed = true;
  }

}
