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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class CassandraFileSystem extends FileSystem
{
    private static final Logger           logger = Logger.getLogger(CassandraFileSystem.class);

    private URI                           uri;

    public final CassandraFileSystemStore store;

    private Path                          workingDir;

    private long                          subBlockSize;

    public CassandraFileSystem()
    {
        this.store = new CassandraFileSystemThriftStore();
    }

    public void initialize(URI uri, Configuration conf) throws IOException
    {

        super.initialize(uri, conf);

        setConf(conf);
        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDir = new Path("/user", System.getProperty("user.name")).makeQualified(this);

        store.initialize(this.uri, conf);
        subBlockSize = conf.getLong("fs.local.subblock.size", 256L * 1024L);
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public String getName()
    {
        return getUri().toString();
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDir;
    }

    @Override
    public void setWorkingDirectory(Path dir)
    {
        workingDir = makeAbsolute(dir);
    }

    private Path makeAbsolute(Path path)
    {
        if (path.isAbsolute())
        {
            return path;
        }
        return new Path(workingDir, path);
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) throws IOException
    {
        Path absolutePath = makeAbsolute(path);
        List<Path> paths = new ArrayList<Path>();
        do
        {
            paths.add(0, absolutePath);
            absolutePath = absolutePath.getParent();
        }
        while (absolutePath != null);

        boolean result = true;
        for (Path p : paths)
        {
            result &= mkdir(p, permission);
        }
        return result;
    }

    private boolean mkdir(Path path, FsPermission permission) throws IOException
    {
        Path absolutePath = makeAbsolute(path);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null)
        {
            store.storeINode(absolutePath, new INode(System.getProperty("user.name", "none"), System.getProperty(
                    "user.name", "none"), permission, INode.FileType.DIRECTORY, null));
        }
        else if (inode.isFile())
        {
            throw new IOException(String.format("Can't make directory for path %s since it is a file.", absolutePath));
        }
        return true;
    }

    @Override
    public boolean isFile(Path path) throws IOException
    {
        INode inode = store.retrieveINode(makeAbsolute(path));
        if (inode == null)
        {
            return false;
        }
        return inode.isFile();
    }

    private INode checkFile(Path path) throws IOException
    {
        INode inode = store.retrieveINode(makeAbsolute(path));
        if (inode == null)
        {
            throw new IOException("No such file.");
        }
        if (inode.isDirectory())
        {
            throw new IOException("Path " + path + " is a directory.");
        }
        return inode;
    }

    @Override
    public FileStatus[] listStatus(Path f) throws IOException
    {
        Path absolutePath = makeAbsolute(f);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null)
        {
            return null;
        }
        if (inode.isFile())
        {
            return new FileStatus[] { new CassandraFileStatus(f.makeQualified(this), inode) };
        }
        ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
        for (Path p : store.listSubPaths(absolutePath))
        {
            // we shouldn't list ourselves
            if (p.equals(f))
                continue;

            try
            {
                FileStatus stat = getFileStatus(p.makeQualified(this));

                ret.add(stat);
            }
            catch (FileNotFoundException e)
            {
                logger.warn("No file found for: " + p);
            }
        }
        return ret.toArray(new FileStatus[0]);
    }

    /** This optional operation is not yet supported. */
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException
    {
        throw new IOException("Not supported");
    }

    /**
     * @param permission
     *            Currently ignored.
     */
    @Override
    public FSDataOutputStream create(Path file, FsPermission permission, boolean overwrite, int bufferSize,
            short replication, long blockSize, Progressable progress) throws IOException
    {

        INode inode = store.retrieveINode(makeAbsolute(file));
        if (inode != null)
        {
            if (overwrite)
            {
                delete(file);
            }
            else
            {
                throw new IOException("File already exists: " + file);
            }
        }
        else
        {
            Path parent = file.getParent();
            if (parent != null)
            {
                if (!mkdirs(parent))
                {
                    throw new IOException("Mkdirs failed to create " + parent.toString());
                }
            }
        }
        return new FSDataOutputStream(new CassandraOutputStream(getConf(), store, makeAbsolute(file), permission,
                blockSize, subBlockSize, progress, bufferSize), statistics);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException
    {
        INode inode = checkFile(path);
        return new FSDataInputStream(new CassandraInputStream(getConf(), store, inode, statistics));
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Renaming " + src + " to " + dst);

        Path absoluteSrc = makeAbsolute(src);
        INode srcINode = store.retrieveINode(absoluteSrc);
        if (srcINode == null)
        {
            // src path doesn't exist
            return false;
        }
        Path absoluteDst = makeAbsolute(dst);
        INode dstINode = store.retrieveINode(absoluteDst);
        if (dstINode != null && dstINode.isDirectory())
        {
            absoluteDst = new Path(absoluteDst, absoluteSrc.getName());
            dstINode = store.retrieveINode(absoluteDst);
        }
        if (dstINode != null)
        {
            // dst path already exists - can't overwrite
            return false;
        }
        Path dstParent = absoluteDst.getParent();
        if (dstParent != null)
        {
            INode dstParentINode = store.retrieveINode(dstParent);
            if (dstParentINode == null || dstParentINode.isFile())
            {
                // dst parent doesn't exist or is a file
                return false;
            }
        }
        return renameRecursive(absoluteSrc, absoluteDst);
    }

    private boolean renameRecursive(Path src, Path dst) throws IOException
    {
        INode srcINode = store.retrieveINode(src);

        Set<Path> paths = store.listDeepSubPaths(src);

        store.storeINode(dst, srcINode);

        for (Path oldSrc : paths)
        {
            INode inode = store.retrieveINode(oldSrc);
            if (inode == null)
            {
                return false;
            }
            String oldSrcPath = oldSrc.toUri().getPath();
            String srcPath = src.toUri().getPath();
            String dstPath = dst.toUri().getPath();
            Path newDst = new Path(oldSrcPath.replaceFirst(srcPath, dstPath));
            store.storeINode(newDst, inode);
            store.deleteINode(oldSrc);
        }

        if (!paths.contains(src))
            store.deleteINode(src);

        return true;
    }

    public boolean delete(Path path, boolean recursive) throws IOException
    {
        if (logger.isDebugEnabled())
            logger.debug("Deleting " + path + " " + recursive);

        Path absolutePath = makeAbsolute(path);
        INode inode = store.retrieveINode(absolutePath);
        if (inode == null)
        {
            return false;
        }
        if (inode.isFile())
        {
            store.deleteINode(absolutePath);
            store.deleteSubBlocks(inode);
        }
        else
        {

            FileStatus[] contents = listStatus(absolutePath);
            if (contents == null)
            {
                return false;
            }
            if ((contents.length != 0) && (!recursive))
            {
                throw new IOException("Directory " + path.toString() + " is not empty.");
            }
            for (FileStatus p : contents)
            {
                if (!delete(p.getPath(), recursive))
                {
                    return false;
                }
            }
            store.deleteINode(absolutePath);

        }
        return true;
    }

    @Override
    @Deprecated
    public boolean delete(Path path) throws IOException
    {
        return delete(path, true);
    }

    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException
    {
        if (file == null)
        {
            return null;
        }

        if (!(file instanceof CassandraFileStatus))
        {
            return super.getFileBlockLocations(file, start, len);
        }

        if ((start < 0) || (len < 0))
        {
            throw new IllegalArgumentException("Invalid start or len parameter");
        }

        if (file.getLen() < start)
        {
            return null;
        }

        INode inode = ((CassandraFileStatus) file).inode;

        long end = start + len;

        if (logger.isDebugEnabled())
        {
            logger.debug(String.format("Looking up Blocks Start: %d Len: %d", start, len));
        }

        List<Block> usedBlocks = new ArrayList<Block>();
        for (Block block : inode.getBlocks())
        {

            // See if the two windows overlap
            if (((start >= block.offset && start < (block.offset + block.length)) || (end >= block.offset && end < (block.offset + block.length)))
                    || ((block.offset >= start && block.offset < end) || ((block.offset + block.length) >= start && (block.offset + block.length) < end)))
            {
                usedBlocks.add(block);
            }
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("Blocks used:");
            printBlocksDebug(usedBlocks);
        }

        return store.getBlockLocation(usedBlocks, start, len);
    }

    /**
     * Print this List by invoking its objects' toString(); using the logger in
     * debug mode.
     * 
     * @param blocks
     *            list of blocks to be printed
     */
    private void printBlocksDebug(List<Block> blocks)
    {
        for (Block block : blocks)
        {
            logger.debug(block);
        }
    }

    /**
     * FileStatus for Cassandra file systems. {@inheritDoc}
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException
    {
        INode inode = store.retrieveINode(makeAbsolute(f));
        if (inode == null)
        {
            throw new FileNotFoundException(f.toString());
        }
        return new CassandraFileStatus(f.makeQualified(this), inode);
    }

    private static class CassandraFileStatus extends FileStatus
    {

        public final INode inode;

        CassandraFileStatus(Path f, INode inode) throws IOException
        {
            super(findLength(inode), inode.isDirectory(), 1, findBlocksize(inode), inode.mtime, f);

            this.inode = inode;
            this.setGroup(inode.group);
            this.setOwner(inode.user);
            this.setPermission(inode.perms);
        }

        private static long findLength(INode inode)
        {
            if (!inode.isDirectory())
            {
                long length = 0L;
                for (Block block : inode.getBlocks())
                {
                    length += block.length;
                }
                return length;
            }
            return 0;
        }

        private static long findBlocksize(INode inode)
        {
            final Block[] ret = inode.getBlocks();
            return ret == null ? 0L : ret[0].length;
        }
    }

}
