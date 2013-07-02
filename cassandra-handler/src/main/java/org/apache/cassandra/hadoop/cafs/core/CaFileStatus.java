package org.apache.cassandra.hadoop.cafs.core;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class CaFileStatus extends FileStatus {

  public final INode inode;

  CaFileStatus(Path f, INode inode) throws IOException {
    super(findLength(inode), inode.isDirectory(), 1, findBlocksize(inode), inode.mtime, f);

    this.inode = inode;
    this.setGroup(inode.group);
    this.setOwner(inode.user);
    this.setPermission(inode.perms);
  }

  private static long findLength(INode inode) {
    if (!inode.isDirectory()) {
      long length = 0L;
      for (Block block : inode.getBlocks()) {
        length += block.length;
      }
      return length;
    }
    return 0;
  }

  private static long findBlocksize(INode inode) {
    final Block[] ret = inode.getBlocks();
    return ret == null ? 0L : ret[0].length;
  }
}