package org.apache.cassandra.hadoop.cafs.core;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cassandra.CassandraException;
import org.apache.hadoop.hive.cassandra.CassandraProxyClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * CaFileSystem Integration Test
 * <p/>
 * <p/>
 * <p/>
 * You must have a Cassandra server running at Localhost However, DO NOT run on any production machine as I will wipe
 * out the whole directory before and after for a clean slate test.
 *
 * @author Feiyi Wang
 */
public class CaFileSystemTest {

  private static Logger logger = LoggerFactory.getLogger(CaFileSystemTest.class);

  private static Cassandra.Client client;
  private static CaFileSystem cafs;

  private static void fillArray(char[] buf) {
    for (int j = 0; j < buf.length; j++)
      buf[j] = (char) j;
  }

  private void setupTestFile(int kUnit) throws IOException {
    cafs.mkdirs(new Path("/opt2"));

    // Create a 1MB file to sent to fs
    File tmp = File.createTempFile("test-cafs", ".dat");

    Writer writer = new FileWriter(tmp);
    char buf[] = new char[1024];
    fillArray(buf);
    for (int i = 0; i < kUnit; i++) {
      writer.write(buf);
    }
    writer.close();
    tmp.deleteOnExit();
    // write file
    Path src = new Path("file://" + tmp.getAbsolutePath());
    Path dst = new Path("/opt2/testfile");

    // verify file status
    cafs.copyFromLocalFile(src, dst);
  }

  @BeforeClass
  public static void setup() throws IOException, CassandraException {

    // System.setProperty("cassandra.config", "conf");
    //client = CassandraClient.createConnection("localhost", 9160);
    client = new CassandraProxyClient("localhost", 9160, true, true).getClientHolder().getClient();
    assertNotNull(client);

    cafs = new CaFileSystem();
    URI cafsURI = URI.create("cafs://localhost:9160/");

    Configuration c = new Configuration();
    cafs.initialize(cafsURI, c);
    assertNotNull(cafs);

  }

  @Test
  public void mkdirs() throws IOException {
    cafs.mkdirs(new Path("/opt1"));
    cafs.mkdirs(new Path("/opt1/sub1"));
    cafs.mkdirs(new Path("/opt1/sub2"));
    cafs.mkdirs(new Path("/opt1/sub1/sub1-1"));

    Set<Path> allPaths = cafs.store.listDeepSubPaths(new Path("/opt1"));

    // verify deep paths
    assertEquals(3, allPaths.size());
  }


  @Test
  public void rmdirs() throws IOException {
    cafs.mkdirs(new Path("/opt3/sub3"));
    cafs.mkdirs(new Path("/opt3/sub3-1"));

    cafs.delete(new Path("/opt3/sub3-1"));

    Set<Path> allPaths = cafs.store.listSubPaths(new Path("/opt3"));
    assertEquals(1, allPaths.size());
  }

  @Test
  public void writeFile() throws IOException {
    setupTestFile(4);
    assertTrue(cafs.isFile(new Path("/opt/testfile")));

    // verify file length
    FileStatus fs = cafs.getFileStatus(new Path("/opt/testfile"));


  }

  @Test
  public void write16bytes() throws IOException {
    cafs.mkdirs(new Path("/opt4"));

    // Create a 1MB file to sent to fs
    File tmp = File.createTempFile("test-cafs", ".dat");

    Writer writer = new FileWriter(tmp);
    char buf[] = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    writer.write(buf);
    writer.close();
    tmp.deleteOnExit();
    // write file
    Path src = new Path("file://" + tmp.getAbsolutePath());
    Path dst = new Path("/opt4/testfile");

    // verify file status
    cafs.copyFromLocalFile(src, dst);

    assert (cafs.isFile(new Path("/opt4/testfile")));
    //assertEquals(cafs.getContentSummary(new Path("/opt4/testfile")).getLength(), buf.length);
  }

  @Test
  public void readFile() throws IOException {
    cafs.mkdirs(new Path("/opt2"));

    // Create a 1MB file to sent to fs
    File tmp = File.createTempFile("test-cafs", ".dat");

    Writer writer = new FileWriter(tmp);
    char buf[] = new char[1024];
    fillArray(buf);
    for (int i = 0; i < 5; i++) {
      writer.write(buf);
    }
    writer.close();
    tmp.deleteOnExit();
    // write file
    Path src = new Path("file://" + tmp.getAbsolutePath());
    Path dst = new Path("/opt2/testfile");

    // verify file status
    cafs.copyFromLocalFile(src, dst);

    assert (cafs.isFile(dst));
  }

  //@After
  public void cleanup() throws IOException {
    cafs.delete(new Path("/"));
    Set<Path> allPaths = cafs.store.listDeepSubPaths(new Path("/"));
    assertEquals(0, allPaths.size());

  }
}
