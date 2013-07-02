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
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.hadoop.ColumnFamilyOutputFormat;
import org.apache.cassandra.hadoop.cafs.core.CaSubBlockInputStream;
import org.apache.cassandra.hadoop.cafs.core.CassandraClient;
import org.apache.cassandra.hadoop.cafs.core.LocalBlock;
import org.apache.cassandra.hadoop.cafs.core.LocalOrRemoteBlock;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MappedFileDataInput;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyException;

/**
 * CFs schema:
 * <p/>
 * Column Families:
 * - inode
 * - sblocks
 * <p/>
 * -------------------
 * |      inode       |
 * -------------------
 * {key : [<path>: <  > ], [<sentinel>: <   >], [ <datacol> : < all blocks with its subBlocks serialized>] }
 * <p/>
 * ------------------
 * |     sblocks      |
 * ------------------
 * { key(Block UUID): [<subBlockUUID> : <data>>], [<subBlockUUID> : <data>>], .......[<subBlockUUID> : <data>>] }
 */
public class CassandraFileSystemThriftStore implements CassandraFileSystemStore {
  private final static Logger logger = Logger.getLogger(CassandraFileSystemThriftStore.class);

  private static final String keySpace = "cfs";

  // Cfs for normal use. They can be overridden if the archive mode is set.
  private static String inodeDefaultCf = "inode";
  private static String sblockDefaultCf = "sblocks";

  // Cfs for archive kind of storage
  private static final String inodeArchiveCf = "inode_archive";
  private static final String sblockArchiveCf = "sblocks_archive";

  private static final ByteBuffer dataCol = ByteBufferUtil.bytes("data");
  private static final ByteBuffer pathCol = ByteBufferUtil.bytes("path");
  private static final ByteBuffer parentPathCol = ByteBufferUtil.bytes("parent_path");
  private static final ByteBuffer sentCol = ByteBufferUtil.bytes("sentinel");


  private String inodeCfInUse = null;
  private String sblockCfInUse = null;

  // This values can be overridden if the archive mode is set.
  private ColumnPath sblockPath = null;
  private ColumnParent sblockParent = null;

  // This values can be overridden if the archive mode is set.
  private ColumnPath inodePath = null;
  private ColumnParent inodeParent = null;

  // This values can be overridden if the archive mode is set.
  private ColumnPath inodeDataPath = null;
  private ColumnPath sblockDataPath = null;

  private ByteBuffer compressedData = null;
  private ByteBuffer uncompressedData = null;

  private StorageType storageTypeInUse = StorageType.CFS_REGULAR;

  private static final SlicePredicate pathPredicate = new SlicePredicate().setColumn_names(Arrays.asList(pathCol));

  private static final ByteBuffer sentinelValue = ByteBufferUtil.bytes("x");

  private ConsistencyLevel consistencyLevelRead;

  private ConsistencyLevel consistencyLevelWrite;

  private Cassandra.Client client;

  public CassandraFileSystemThriftStore() {

  }

  public void initialize(URI uri, Configuration conf) throws IOException {

    String host = uri.getHost();
    int port = uri.getPort();

    if (host == null || host.isEmpty() || host.equals("null"))
      host = FBUtilities.getLocalAddress().getHostName();

    if (port == -1)
      port = DatabaseDescriptor.getRpcPort(); // default

    client = CassandraClient.createConnection(host, port);

    KsDef ks = checkKeyspace();

    if (ks == null)
      ks = createKeySpace();

    initConsistencyLevels(ks, conf);
    initCFNames(uri);

    try {
      client.set_keyspace(keySpace);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Set to different set of Column Families is the archive location is selected.
   */
  private void initCFNames(URI uri) {

    if (isArchive(uri)) {
      // cfs-archive:///
      inodeCfInUse = inodeArchiveCf;
      sblockCfInUse = sblockArchiveCf;

      storageTypeInUse = StorageType.CFS_ARCHIVE;
    } else {
      // cfs:///
      inodeCfInUse = inodeDefaultCf;
      sblockCfInUse = sblockDefaultCf;
    }

    // Create the remaining paths and parents base on the CfInUse.

    sblockPath = new ColumnPath(sblockCfInUse);
    sblockParent = new ColumnParent(sblockCfInUse);

    inodePath = new ColumnPath(inodeCfInUse);
    inodeParent = new ColumnParent(inodeCfInUse);

    inodeDataPath = new ColumnPath(inodeCfInUse).setColumn(dataCol);
    sblockDataPath = new ColumnPath(sblockCfInUse).setColumn(dataCol);

  }

  /**
   * Returns TRUE is the <code>uri</code> correspond to an archive location.
   */
  private boolean isArchive(URI uri) {
    return uri.getScheme().startsWith("cfs-archive");
  }

  /**
   * Initialize the consistency levels for reads and writes.
   *
   * @param ks Keyspace definition
   */
  private void initConsistencyLevels(KsDef ks, Configuration conf) {

    consistencyLevelRead = ConsistencyLevel.valueOf(conf.get("cfs.consistencylevel.read", "ONE"));
    consistencyLevelWrite = ConsistencyLevel.valueOf(conf.get("cfs.consistencylevel.write", "ONE"));

    // Change consistency if this using NTS
    if (ks.getStrategy_class().contains("NetworkTopologyStrategy")) {
      if (consistencyLevelRead.equals(ConsistencyLevel.QUORUM)) {
        consistencyLevelRead = ConsistencyLevel.LOCAL_QUORUM;
      }
      if (consistencyLevelWrite.equals(ConsistencyLevel.QUORUM)) {
        consistencyLevelWrite = ConsistencyLevel.LOCAL_QUORUM;
      }
    }
  }

  private KsDef checkKeyspace() throws IOException {
    try {
      return client.describe_keyspace(keySpace);
    } catch (NotFoundException e) {
      return null;
    } catch (InvalidRequestException e) {
      throw new IOException(e);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  public KsDef createKeySpace() throws IOException {
    try {
      // Stagger create time so nodes don't
      // get confused
      Thread.sleep(new Random().nextInt(5000));

      KsDef cfsKs = checkKeyspace();

      if (cfsKs != null)
        return cfsKs;

      List<CfDef> cfs = new ArrayList<CfDef>();

      CfDef cf = new CfDef();
      cf.setName(inodeDefaultCf);
      cf.setComparator_type("BytesType");
      cf.setKey_cache_size(1000000);
      cf.setRow_cache_size(0);
      cf.setGc_grace_seconds(60);
      cf.setComment("Stores file meta data");
      cf.setKeyspace(keySpace);

      cf.setColumn_metadata(
              Arrays.asList(new ColumnDef(pathCol, "BytesType").
                      setIndex_type(IndexType.KEYS).
                      setIndex_name("path"),
                      new ColumnDef(sentCol, "BytesType").
                              setIndex_type(IndexType.KEYS).
                              setIndex_name("sentinel"),
                      new ColumnDef(parentPathCol, "BytesType").
                              setIndex_type(IndexType.KEYS).
                              setIndex_name("parent_path")));

      cfs.add(cf);

      cf = new CfDef();
      cf.setName(sblockDefaultCf);
      cf.setComparator_type("BytesType");
      cf.setKey_cache_size(1000000);
      cf.setRow_cache_size(0);
      cf.setGc_grace_seconds(60);
      cf.setComment("Stores blocks of information associated with a inode");
      cf.setKeyspace(keySpace);

      cf.setMin_compaction_threshold(16);
      cf.setMax_compaction_threshold(64);

      cfs.add(cf);

      // CFs for archive
      cf = new CfDef();
      cf.setName(inodeArchiveCf);
      cf.setComparator_type("BytesType");
      cf.setKey_cache_size(1000000);
      cf.setRow_cache_size(0);
      cf.setGc_grace_seconds(60);
      cf.setComment("Stores file meta data");
      cf.setKeyspace(keySpace);

      cf.setColumn_metadata(
              Arrays.asList(new ColumnDef(pathCol, "BytesType").
                      setIndex_type(IndexType.KEYS).
                      setIndex_name("path"),
                      new ColumnDef(sentCol, "BytesType").
                              setIndex_type(IndexType.KEYS).
                              setIndex_name("sentinel"),
                      new ColumnDef(parentPathCol, "BytesType").
                              setIndex_type(IndexType.KEYS).
                              setIndex_name("parent_path")));

      cfs.add(cf);

      cf = new CfDef();
      cf.setName(sblockArchiveCf);
      cf.setComparator_type("BytesType");
      cf.setKey_cache_size(1000000);
      cf.setRow_cache_size(0);
      cf.setGc_grace_seconds(60);
      cf.setComment("Stores blocks of information associated with a inode");
      cf.setKeyspace(keySpace);

      // Disable compaction for archive.
      cf.setMin_compaction_threshold(0);
      cf.setMax_compaction_threshold(0);

      cfs.add(cf);

      Map<String, String> stratOpts = new HashMap<String, String>();
      stratOpts.put("replication_factor", System.getProperty("cafs.replication", "1"));

      cfsKs = new KsDef()
              .setName(keySpace)
              .setStrategy_class("org.apache.cassandra.locator.NetworkTopologyStrategy")
              .setStrategy_options(stratOpts)
              .setReplication_factor(Integer.parseInt(System.getProperty("cfs.replication", "1")))
              .setDurable_writes(System.getProperty("cfs.replication", "1").equals("1") ? true : false)
              .setCf_defs(cfs);

      client.system_add_keyspace(cfsKs);
      waitForSchemaAgreement(client);

      return cfsKs;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


  public InputStream retrieveBlock(Block block, long byteRangeStart) throws IOException {
    return new CassandraSubBlockInputStream(this, block, byteRangeStart);
  }

  public InputStream retrieveSubBlock(Block block, SubBlock subBlock, long byteRangeStart) throws IOException {
    ByteBuffer blockId = uuidToByteBuffer(block.id);
    ByteBuffer subBlockId = uuidToByteBuffer(subBlock.id);

    LocalOrRemoteBlock blockData = null;

    try {
      blockData = get_cfs_sblock(FBUtilities.getLocalAddress().getHostName(),
              blockId, subBlockId, (int) 0, storageTypeInUse);
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (blockData == null)
      throw new IOException("Missing block: " + subBlock.id);

    InputStream is = null;
    if (blockData.remote_block != null)
      is = getInputStream(blockData.remote_block);
    else
      is = readLocalBlock(blockData.getLocal_block());

    if (byteRangeStart > 0)
      is.skip(byteRangeStart);

    return is;
  }

  private synchronized InputStream getInputStream(ByteBuffer bb) throws IOException {

    ByteBuffer output = null;

    if (compressedData == null || compressedData.capacity() < bb.remaining())
      compressedData = ByteBuffer.allocateDirect(bb.remaining());

    compressedData.limit(compressedData.capacity());
    compressedData.rewind();
    compressedData.put(bb.duplicate());
    compressedData.limit(compressedData.position());
    compressedData.rewind();

    if (Snappy.isValidCompressedBuffer(compressedData)) {

      int uncompressedLength = Snappy.uncompressedLength(compressedData);

      if (uncompressedData == null || uncompressedData.capacity() < uncompressedLength) {
        uncompressedData = ByteBuffer.allocateDirect(uncompressedLength);
      }

      int len = Snappy.uncompress(compressedData, uncompressedData);

      uncompressedData.limit(len);
      uncompressedData.rewind();

      output = uncompressedData;
    } else {
      output = compressedData;
    }

    return ByteBufferUtil.inputStream(output);
  }

  private InputStream readLocalBlock(LocalBlock blockInfo) throws IOException {

    if (blockInfo.file == null)
      throw new RuntimeException("Local file name is not defined");

    if (blockInfo.length == 0)
      return ByteBufferUtil.inputStream(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(blockInfo.file, "r");

      if (logger.isDebugEnabled())
        logger.debug("Mmapping " + blockInfo.length + " bytes");

      MappedByteBuffer bb = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, blockInfo.offset,
              blockInfo.length);

      return getInputStream(bb);

    } catch (FileNotFoundException e) {
      throw new RuntimeException("Local file does not exist: " + blockInfo.file);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Unable to mmap block %s[%d,%d]", blockInfo.file,
              blockInfo.length, blockInfo.offset), e);
    } finally {
      FileUtils.closeQuietly(raf);
    }

  }

  public INode retrieveINode(Path path) throws IOException {
    ByteBuffer pathKey = getPathKey(path);
    ColumnOrSuperColumn pathInfo;

    pathInfo = performGet(pathKey, inodeDataPath, consistencyLevelRead);

    // If not found and I already tried with CL= ONE, retry with higher CL.
    if (pathInfo == null && consistencyLevelRead.equals(ConsistencyLevel.ONE)) {
      pathInfo = performGet(pathKey, inodeDataPath, ConsistencyLevel.QUORUM);
    }

    if (pathInfo == null) {
      // Now give up and return null.
      return null;
    }

    return INode.deserialize(ByteBufferUtil.inputStream(pathInfo.column.value), pathInfo.column.getTimestamp());
  }

  private ColumnOrSuperColumn performGet(ByteBuffer key, ColumnPath cp, ConsistencyLevel cl) throws IOException {
    ColumnOrSuperColumn result;
    try {
      result = client.get(key, cp, cl);
    } catch (NotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new IOException(e);
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  public synchronized void storeSubBlock(UUID parentBlockUUID, SubBlock sblock, ByteBuffer data) throws IOException {
    assert parentBlockUUID != null;

    // Row key is the Block id to which this SubBLock belongs to.
    ByteBuffer parentBlockId = uuidToByteBuffer(parentBlockUUID);

    //Prepare the buffer to hold the compressed data
    int maxCapacity = Snappy.maxCompressedLength(data.capacity());
    if (compressedData == null || compressedData.capacity() < maxCapacity) {
      compressedData = ByteBuffer.allocateDirect(maxCapacity);
    }

    compressedData.limit(compressedData.capacity());
    compressedData.rewind();

    //compress
    int len = Snappy.compress(data, compressedData);
    compressedData.limit(len);
    compressedData.rewind();

    if (logger.isDebugEnabled()) {
      logger.debug("Storing " + sblock);
    }

    // Row Key: UUID of SubBLock Block parent
    // Column name: Sub Block UUID
    // Column value: Sub Block Data.

    try {
      client.insert(
              parentBlockId,
              sblockParent,
              new Column().setName(uuidToByteBuffer(sblock.id)).setValue(compressedData).setTimestamp(System.currentTimeMillis()),
              consistencyLevelWrite);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void storeINode(Path path, INode inode) throws IOException {

    if (logger.isDebugEnabled() && inode.getBlocks() != null) {
      logger.debug("Writing inode to: " + path);
      printBlocksDebug(inode.getBlocks());
    }

    // Inode row key
    ByteBuffer pathKey = getPathKey(path);

    ByteBuffer data = inode.serialize();

    Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
    Map<String, List<Mutation>> pathMutations = new HashMap<String, List<Mutation>>();
    List<Mutation> mutations = new ArrayList<Mutation>();

    // setup mutation map
    pathMutations.put(inodeCfInUse, mutations);
    mutationMap.put(pathKey, pathMutations);

    long ts = System.currentTimeMillis();

    // file name
    mutations.add(createMutationForCol(pathCol, ByteBufferUtil.bytes(path.toUri().getPath()), ts));

    // Parent name for this file
    mutations.add(createMutationForCol(parentPathCol, ByteBufferUtil.bytes(getParentForIndex(path)), ts));

    // sentinal
    mutations.add(createMutationForCol(sentCol, sentinelValue, ts));

    // serialized inode
    mutations.add(createMutationForCol(dataCol, data, ts));

    try {
      client.batch_mutate(mutationMap, consistencyLevelWrite);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @param path a Path
   * @return the parent to the <code>path</code> or null if the <code>path</code> represents the root.
   */
  private String getParentForIndex(Path path) {
    Path parent = path.getParent();

    if (parent == null) {
      return "null";
    }

    return parent.toUri().getPath();
  }

  /**
   * Creates a mutation for a column <code>colName</code> whose value is <code>value</code> and with
   * tiemstamp <code>ts</code>.
   *
   * @param colName column name
   * @param value   column value
   * @param ts      column timestamp
   * @return a Mutation object
   */
  private Mutation createMutationForCol(ByteBuffer colName, ByteBuffer value, long ts) {
    return new Mutation().setColumn_or_supercolumn(
            new ColumnOrSuperColumn().setColumn(
                    new Column().setName(colName).
                            setValue(value).
                            setTimestamp(ts)));
  }

  /**
   * Print this List by invoking its objects' toString(); using the logger in debug mode.
   *
   * @param blocks list of blocks to be printed
   */
  private void printBlocksDebug(Block[] blocks) {
    for (Block block : blocks) {
      logger.debug(block);
    }
  }

  ByteBuffer getPathKey(Path path) {
    return ByteBufferUtil.bytes(FBUtilities.hashToBigInteger(ByteBufferUtil.bytes(path.toUri().getPath()))
            .toString(16));
  }


  ByteBuffer uuidToByteBuffer(UUID id) {
    return ByteBufferUtil.bytes(Hex.bytesToHex(UUIDGen.decompose(id)));
  }

  /**
   * {@inheritDoc}
   */
  public void deleteSubBlocks(INode inode) throws IOException {
    // Get all the SubBlock keys to delete.
    List<UUID> subBlockKeys = getListOfBlockIds(inode.getBlocks());
    try {
      // TODO (patricioe) can we send one big batch mutation  here ?
      for (UUID subBlocksKey : subBlockKeys) {
        client.remove(ByteBuffer.wrap(UUIDGen.decompose(subBlocksKey)), sblockPath, System.currentTimeMillis(),
                consistencyLevelWrite);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Retrieves a list of UUIDs
   *
   * @param blocks list of blocks
   * @return a list of UUID
   */
  private List<UUID> getListOfBlockIds(Block[] blocks) {
    List<UUID> blockIds = new ArrayList<UUID>(blocks.length);
    for (Block aBlock : blocks) {
      blockIds.add(aBlock.id);
    }
    return blockIds;
  }

  /**
   * {@inheritDoc}
   */
  public void deleteINode(Path path) throws IOException {
    try {
      client.remove(getPathKey(path), inodePath, System.currentTimeMillis(), consistencyLevelWrite);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public Set<Path> listDeepSubPaths(Path path) throws IOException {
    String startPath = path.toUri().getPath();

    List<IndexExpression> indexExpressions = new ArrayList<IndexExpression>();

    indexExpressions.add(new IndexExpression(sentCol, IndexOperator.EQ, sentinelValue));
    indexExpressions.add(new IndexExpression(pathCol, IndexOperator.GT, ByteBufferUtil.bytes(startPath)));

    // Limit listings to this root by incrementing the last char
    if (startPath.length() > 1) {
      String endPath = startPath.substring(0, startPath.length() - 1)
              + new Character((char) (startPath.charAt(startPath.length() - 1) + 1));

      indexExpressions.add(new IndexExpression(pathCol, IndexOperator.LT, ByteBufferUtil.bytes(endPath)));
    }

    try {
      List<KeySlice> keys = client.get_indexed_slices(inodeParent, new IndexClause(indexExpressions,
              ByteBufferUtil.EMPTY_BYTE_BUFFER, 100000), pathPredicate, consistencyLevelRead);

      Set<Path> matches = new HashSet<Path>(keys.size());

      for (KeySlice key : keys) {
        for (ColumnOrSuperColumn cosc : key.getColumns()) {
          matches.add(new Path(ByteBufferUtil.string(cosc.column.value)));
        }
      }

      return matches;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }


  public Set<Path> listSubPaths(Path path) throws IOException {

    String startPath = path.toUri().getPath();

    List<IndexExpression> indexExpressions = new ArrayList<IndexExpression>();

    indexExpressions.add(new IndexExpression(sentCol, IndexOperator.EQ, sentinelValue));
    indexExpressions.add(new IndexExpression(parentPathCol, IndexOperator.EQ, ByteBufferUtil.bytes(startPath)));

    try {
      List<KeySlice> keys = client.get_indexed_slices(inodeParent, new IndexClause(indexExpressions,
              ByteBufferUtil.EMPTY_BYTE_BUFFER, 100000), pathPredicate, consistencyLevelRead);

      Set<Path> matches = new HashSet<Path>(keys.size());

      for (KeySlice key : keys) {
        for (ColumnOrSuperColumn cosc : key.getColumns()) {
          matches.add(new Path(ByteBufferUtil.string(cosc.column.value)));
        }
      }

      return matches;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public String getVersion() throws IOException {
    return "Cassandra FileSystem Thrift Store";
  }

  public BlockLocation[] getBlockLocation(List<Block> blocks, long start, long len) throws IOException {
    if (blocks.isEmpty())
      return null;

    List<ByteBuffer> blockKeys = new ArrayList<ByteBuffer>(blocks.size());

    for (Block b : blocks)
      blockKeys.add(uuidToByteBuffer(b.id));

    BlockLocation[] locations = new BlockLocation[blocks.size()];

    try {
      List<List<String>> blockEndpoints = describe_keys(keySpace, blockKeys);

      for (int i = 0; i < blockEndpoints.size(); i++) {
        List<String> endpoints = blockEndpoints.get(i);
        Block b = blocks.get(i);

        long offset = (i == 0 && b.offset > start) ? start : b.offset;

        // TODO: Add topology info if at all possible?
        locations[i] = new BlockLocation(null, endpoints.toArray(new String[0]), offset, b.length);
      }

      return locations;
    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  public static void waitForSchemaAgreement(Cassandra.Client aClient)
          throws InvalidRequestException, InterruptedException, TException {
    int waited = 0;
    int versions = 0;
    while (versions != 1) {
      ArrayList<String> liveschemas = new ArrayList<String>();
      Map<String, List<String>> schema = aClient
              .describe_schema_versions();
      for (Map.Entry<String, List<String>> entry : schema.entrySet()) {
        if (!entry.getKey().equals("UNREACHABLE"))
          liveschemas.add(entry.getKey());
      }
      versions = liveschemas.size();
      Thread.sleep(1000);
      waited += 1000;
      if (waited > StorageService.RING_DELAY)
        throw new RuntimeException(
                "Could not reach schema agreement in "
                        + StorageService.RING_DELAY + "ms");
    }
  }


  private LocalOrRemoteBlock get_cfs_sblock(String callerHostName, String subBlockCFName, ByteBuffer blockId,
                                            ByteBuffer sblockId, int offset, ColumnParent subBlockDataPath) throws TException, TimedOutException,
          UnavailableException, InvalidRequestException, NotFoundException, ReadTimeoutException, org.apache.cassandra.exceptions.UnavailableException, IOException {

    // This logic is only used on mmap spec machines
    if (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.mmap) {
      if (logger.isDebugEnabled())
        logger.debug("Checking for local block: " + blockId + " from " + callerHostName + " on "
                + FBUtilities.getLocalAddress().getHostName());

      List<String> hosts = getKeyLocations(blockId);
      boolean isLocal = false;

      for (String hostName : hosts) {
        if (logger.isDebugEnabled())
          logger.debug("Block " + blockId + " lives on " + hostName);

        if (hostName.equals(callerHostName) && hostName.equals(FBUtilities.getLocalAddress().getHostName())) {
          isLocal = true;

          break;
        }
      }

      if (isLocal) {
        if (logger.isDebugEnabled())
          logger.debug("Local block should be on this node " + blockId);

        LocalBlock localBlock = getLocalSubBlock(subBlockCFName, blockId, sblockId, offset);

        if (localBlock != null) {
          if (logger.isDebugEnabled())
            logger.debug("Local block found: " + localBlock);

          return new LocalOrRemoteBlock().setLocal_block(localBlock);
        }
      }
    }

    if (logger.isDebugEnabled())
      logger.debug("Checking for remote block: " + blockId);

    // Fallback to storageProxy
    return getRemoteSubBlock(blockId, sblockId, offset, subBlockDataPath);

  }

  public LocalOrRemoteBlock get_cfs_sblock(String callerHostName, ByteBuffer blockId, ByteBuffer sblockId,
                                           int offset,
                                           StorageType storageType) throws InvalidRequestException, UnavailableException, TimedOutException,
          NotFoundException,
          TException, ReadTimeoutException, org.apache.cassandra.exceptions.UnavailableException, IOException {

    /*if (storageType == StorageType.CFS_REGULAR) {*/
    return get_cfs_sblock(callerHostName, sblockCfInUse, blockId, sblockId, offset,
            sblockParent);
    /*} else {
      return get_cfs_sblock(callerHostName, sblockArchiveCf, blockId, sblockId, offset,
              subBlockArchiveDataPath);
    } */

  }

  private LocalOrRemoteBlock getRemoteSubBlock(
          ByteBuffer blockId,
          ByteBuffer sblockId,
          int offset,
          ColumnParent subBlockDataPath)
          throws IOException, org.apache.cassandra.exceptions.UnavailableException, NotFoundException, InvalidRequestException, ReadTimeoutException {
    // The column name is the SubBlock id (UUID)
    ReadCommand rc = new SliceByNamesReadCommand(keySpace, blockId, subBlockDataPath, Arrays.asList(sblockId));

    LocalOrRemoteBlock block = new LocalOrRemoteBlock();

    try {
      // CL=ONE as there are NOT multiple versions of the blocks.
      List<Row> rows = StorageProxy.read(Arrays.asList(rc), org.apache.cassandra.db.ConsistencyLevel.ONE);

      IColumn col = null;
      try {
        col = validateAndGetColumn(rows, sblockId);
      } catch (NotFoundException e) {
        // This is a best effort to get the value. Sometimes due to the size of
        // the sublocks, the normal replication may time out leaving a replicate without
        // the piece of data. Hence we re try with higher CL.
        rows = StorageProxy.read(Arrays.asList(rc), org.apache.cassandra.db.ConsistencyLevel.QUORUM);
      }

      col = validateAndGetColumn(rows, sblockId);

      ByteBuffer value = col.value();

      if (value.remaining() < offset)
        throw new InvalidRequestException("Invalid offset for block of size: " + value.remaining());

      if (offset > 0) {
        ByteBuffer offsetBlock = value.duplicate();
        offsetBlock.position(offsetBlock.position() + offset);
        block.setRemote_block(offsetBlock);
      } else {
        block.setRemote_block(value);
      }

    } catch (IsBootstrappingException e) {
      throw new IOException("The node is bootstrapping", e);
    }
    return block;
  }

  /**
   * Validates that the result is not empty and get the value for the <code>columnName</code> column.
   *
   * @param rows       the raw result from StorageProxy.read(....)
   * @param columnName column name
   * @return the Column that was requested if it exists.
   * @throws org.apache.cassandra.thrift.NotFoundException
   *          if the result doesn't exist (including if the value holds a tumbstone)
   */
  public static IColumn validateAndGetColumn(List<Row> rows, ByteBuffer columnName) throws NotFoundException {
    if (rows.isEmpty())
      throw new NotFoundException();

    if (rows.size() > 1)
      throw new RuntimeException("Block id returned more than one row");

    Row row = rows.get(0);
    if (row.cf == null)
      throw new NotFoundException();

    IColumn col = row.cf.getColumn(columnName);

    if (col == null || !col.isLive())
      throw new NotFoundException();

    return col;
  }

  public List<List<String>> describe_keys(String keyspace, List<ByteBuffer> keys) throws TException {
    List<List<String>> keyEndpoints = new ArrayList<List<String>>(keys.size());

    for (ByteBuffer key : keys) {
      keyEndpoints.add(getKeyLocations(key));
    }

    return keyEndpoints;
  }

  private List<String> getKeyLocations(ByteBuffer key) {
    List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(Table.open(keySpace), key);
    DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getLocalAddress(), endpoints);

    List<String> hosts = new ArrayList<String>(endpoints.size());

    for (InetAddress endpoint : endpoints) {
      hosts.add(endpoint.getHostName());
    }

    return hosts;
  }

  /**
   * Retrieves a local subBlock
   *
   * @param blockId  row key
   * @param sblockId SubBlock column name
   * @param offset   inside the sblock
   * @return a local sublock
   * @throws org.apache.thrift.TException
   */
  private LocalBlock getLocalSubBlock(String subBlockCFName, ByteBuffer blockId,
                                      ByteBuffer sblockId, int offset) throws TException {
    DecoratedKey decoratedKey = new DecoratedKey(StorageService.getPartitioner().getToken(blockId), blockId);

    Table table = Table.open(keySpace);
    ColumnFamilyStore sblockStore = table.getColumnFamilyStore(subBlockCFName);

    Collection<SSTableReader> sstables = sblockStore.getSSTables();

    for (SSTableReader sstable : sstables) {

      long position = sstable.getPosition(decoratedKey, SSTableReader.Operator.EQ).position;

      if (position == -1)
        continue;

      String filename = sstable.descriptor.filenameFor(Component.DATA);
      RandomAccessFile raf = null;
      int mappedLength = -1;
      MappedByteBuffer mappedData = null;
      MappedFileDataInput file = null;
      try {
        raf = new RandomAccessFile(filename, "r");
        assert position < raf.length();

        mappedLength = (raf.length() - position) < Integer.MAX_VALUE ? (int) (raf.length() - position)
                : Integer.MAX_VALUE;

        mappedData = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, position, mappedLength);

        file = new MappedFileDataInput(mappedData, filename, 0, -1);

        if (file == null)
          continue;

        //Verify key was found in data file
        DecoratedKey keyInDisk = SSTableReader.decodeKey(sstable.partitioner,
                sstable.descriptor,
                ByteBufferUtil.readWithShortLength(file));
        assert keyInDisk.equals(decoratedKey) : String.format("%s != %s in %s", keyInDisk, decoratedKey, file.getPath());

        long rowSize = SSTableReader.readRowSize(file, sstable.descriptor);

        assert rowSize > 0;
        assert rowSize < mappedLength;

        IndexHelper.skipBloomFilter(file);

        List<IndexHelper.IndexInfo> indexList = IndexHelper.deserializeIndex(file);

        // we can stop early if bloom filter says none of the
        // columns actually exist -- but,
        // we can't stop before initializing the cf above, in
        // case there's a relevant tombstone
        ColumnFamilySerializer serializer = ColumnFamily.serializer;
        try {


          ColumnFamily cf = ColumnFamily.create(sstable.metadata);

          IColumnSerializer columnSerializer = cf.getColumnSerializer();
          cf.delete(DeletionInfo.serializer().deserialize(file, MessagingService.current_version, cf.getComparator()));


          if (cf.isMarkedForDelete())
            continue;

        } catch (Exception e) {
          e.printStackTrace();

          throw new IOException(serializer + " failed to deserialize " + sstable.getColumnFamilyName()
                  + " with " + sstable.metadata + " from " + file, e);
        }


        Integer sblockLength = null;

        if (indexList == null)
          sblockLength = seekToSubColumn(sstable.metadata, file, sblockId);
        else
          sblockLength = seekToSubColumn(sstable.metadata, file, sblockId, indexList);


        if (sblockLength == null || sblockLength < 0)
          continue;


        int bytesReadFromStart = mappedLength - (int) file.bytesRemaining();

        if (logger.isDebugEnabled())
          logger.debug("BlockLength = " + sblockLength + " Availible " + file.bytesRemaining());

        assert offset <= sblockLength : String.format("%d > %d", offset, sblockLength);

        long dataOffset = position + bytesReadFromStart;

        if (file.bytesRemaining() == 0 || sblockLength == 0)
          continue;


        return new LocalBlock(file.getPath(), dataOffset + offset, sblockLength - offset);

      } catch (IOException e) {
        throw new TException(e);
      } finally {
        FileUtils.closeQuietly(raf);
      }
    }


    return null;
  }

  /**
   * Checks if the current column is the one we are looking for
   *
   * @param metadata
   * @param file
   * @param sblockId
   * @return if > 0 the length to read from current file offset. if -1 not relevent. if null out of bounds
   */
  private Integer isSubBlockFound(CFMetaData metadata, FileDataInput file, ByteBuffer sblockId) throws IOException {
    ByteBuffer name = ByteBufferUtil.readWithShortLength(file);

    //Stop if we've gone too far (return null)
    if (metadata.comparator.compare(name, sblockId) > 0)
      return null;

    // verify column type;
    int b = file.readUnsignedByte();

    // skip ts (since we know block ids are unique)
    long ts = file.readLong();
    int sblockLength = file.readInt();

    if (!name.equals(sblockId) || (b & ColumnSerializer.DELETION_MASK) != 0
            || (b & ColumnSerializer.EXPIRATION_MASK) != 0) {
      FileUtils.skipBytesFully(file, sblockLength);
      return -1;
    }

    return sblockLength;
  }

  private Integer seekToSubColumn(CFMetaData metadata, FileDataInput file,
                                  ByteBuffer sblockId, List<IndexHelper.IndexInfo> indexList) throws IOException {
    file.readInt(); // column count

        /* get the various column ranges we have to read */
    AbstractType comparator = metadata.comparator;

    int index = IndexHelper.indexFor(sblockId, indexList, comparator, false, -1);
    if (index == indexList.size())
      return null;

    IndexHelper.IndexInfo indexInfo = indexList.get(index);
    if (comparator.compare(sblockId, indexInfo.firstName) < 0)
      return null;

    FileMark mark = file.mark();

    FileUtils.skipBytesFully(file, indexInfo.offset);

    while (file.bytesPastMark(mark) < indexInfo.offset + indexInfo.width) {
      Integer dataLength = isSubBlockFound(metadata, file, sblockId);

      if (dataLength == null)
        return null;

      if (dataLength < 0)
        continue;

      return dataLength;
    }

    return null;
  }


  //Called when there are is no row index (meaning small number of columns)
  private Integer seekToSubColumn(CFMetaData metadata, FileDataInput file,
                                  ByteBuffer sblockId) throws IOException {
    int columns = file.readInt();
    for (int i = 0; i < columns; i++) {
      Integer dataLength = isSubBlockFound(metadata, file, sblockId);


      if (dataLength == null)
        return null;

      if (dataLength < 0)
        continue;

      return dataLength;

    }

    return null;
  }
}
