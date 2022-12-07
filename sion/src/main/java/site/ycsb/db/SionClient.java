/**
 * Sion client binding for YCSB.
 *
 * All YCSB records are mapped to a Sion *object*.  No scanning support.
 */

package site.ycsb.db;

import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for Sion.
 *
 * Keys are written to an object according to S3 binding. 
 * See {@code s3/README.md} for details.
 */
public class SionClient extends DB {

  private JedisCommands jedis;
  private int maxFields;
  private boolean reset;

  public static final String HOST_PROPERTY = "sion.host";
  public static final String PORT_PROPERTY = "sion.port";
  public static final String TIMEOUT_PROPERTY = "sion.timeout";
  public static final String FIELD_COUNT = "fieldcount";

  public static final String CLUSTER_PROPERTY = "redis.cluster";

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final int DEFAULT_PORT = 6378;
  public static final int DEFAULT_FIELD_COUNT = 10;

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = DEFAULT_PORT;
    }

    String host = props.getProperty(HOST_PROPERTY);
    if (host == null) {
      host = DEFAULT_HOST;
    }
    String[] hosts = host.split(",");

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    // Automatically enables cluster if specified hosts.
    if (hosts.length > 1) {
      clusterEnabled = true;
    }

    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      for (int i = 0; i < hosts.length; i++) {
        jedisClusterNodes.add(new HostAndPort(hosts[i], port));
      }
      jedis = new JedisCluster(jedisClusterNodes);
    } else {
      String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
      if (redisTimeout != null){
        jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
      } else {
        jedis = new Jedis(host, port);
      }
      ((Jedis) jedis).connect();
    }

    String fcntString = props.getProperty(FIELD_COUNT);
    if (fcntString != null) {
      maxFields = Integer.parseInt(fcntString);
    } else {
      maxFields = DEFAULT_FIELD_COUNT;
    }
    reset = false;
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      validate();
      String object = jedis.get(key);
      result.put(key, new ByteArrayByteIterator(object.getBytes()));
    } catch (Exception e) {
      reset();
      System.err.println("Not possible to get the object "+key+ ": "+e.getMessage());
      // e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  /**
  * Create a new Object. Any field/value pairs in the specified
  * values HashMap will be written into the object with the specified record
  * key.
  *
  * @param table
  *            The name of the table, ignored.
  * @param key
  *            The key of the object to insert.
  * @param values
  *            A HashMap of field/value pairs to insert in the object.
  *            Only the content of the first field is written to a byteArray
  *            multiplied by the number of field. In this way the size
  *            of the file to upload is determined by the fieldlength
  *            and fieldcount parameters.
  * @return OK on success, ERROR otherwise. See the
  *         {@link DB} class's description for a discussion of error codes.
  */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    return writeToStorage(key, values, 3);
  }

  /**
  * Update a object in the database. Any field/value pairs in the specified
  * values HashMap will be written into the object with the specified object
  * key, overwriting any existing values with the same field name.
  *
  * @param bucket
  *            The name of the bucket
  * @param key
  *            The file key of the file to write.
  * @param values
  *            A HashMap of field/value pairs to update in the record
  * @return OK on success, ERORR otherwise.
  */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    Status status = writeToStorage(key, values, 1);
    return status;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String bucket, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  protected void reset() {
    try {
      reset = true;
      cleanup();
    } catch (Exception e) {
      System.err.println("Error on reset connection: "+e.getMessage());
    }
  }

  protected void validate() throws DBException {
    if (reset) {
      init();
    }
  }

  protected Status writeToStorage(String key, Map<String, ByteIterator> values, int retries) {
    int totalSize = 0;
    int fieldCount = values.size(); //number of fields to concatenate
    // During update, compensate the missing fields.
    if (fieldCount < maxFields) {
      fieldCount = maxFields;
    }
    // getting the first field in the values
    Object keyToSearch = values.keySet().toArray()[0];
    // getting the content of just one field
    byte[] sourceArray = values.get(keyToSearch).toArray();
    int sizeArray = sourceArray.length; //size of each array
    totalSize = sizeArray*fieldCount;

    byte[] destinationArray = new byte[totalSize];
    int offset = 0;
    for (int i = 0; i < fieldCount; i++) {
      System.arraycopy(sourceArray, 0, destinationArray, offset, sizeArray);
      offset += sizeArray;
    }

    Exception lastErr = null;
    for (int i = 0; i < retries; i++) {
      try {
        validate();
        String ret = jedis.set(key, new String(destinationArray));
        if (ret.equals("OK")) {
          return Status.OK;
        } else {
          throw new Exception("unexpected result");
        }
      } catch (Exception e) {
        lastErr = e;
        reset();
        // Try again
      }
    }
    System.err.println("Not possible to write the object "+key+ ": "+lastErr.getMessage());
    return Status.ERROR;
  }
}
