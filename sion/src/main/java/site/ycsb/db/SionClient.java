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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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

  public static final String HOST_PROPERTY = "sion.host";
  public static final String PORT_PROPERTY = "sion.port";
  public static final String TIMEOUT_PROPERTY = "sion.timeout";

  public static final String DEFAULT_HOST = "127.0.0.1";
  public static final int DEFAULT_PORT = 6378;

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

    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
    if (redisTimeout != null){
      jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
    } else {
      jedis = new Jedis(host, port);
    }
    ((Jedis) jedis).connect();
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
      String object = jedis.get(key);
      result.put(key, new ByteArrayByteIterator(object.getBytes()));
    } catch (Exception e) {
      System.err.println("Not possible to get the object "+key);
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
    return writeToStorage(key, values, 1);
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

  protected Status writeToStorage(String key, Map<String, ByteIterator> values, int retries) {
    int totalSize = 0;
    int fieldCount = values.size(); //number of fields to concatenate
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

    for (int i = 0; i < retries; i++) {
      try {
        if (jedis.set(key, new String(destinationArray)).equals("OK")) {
          return Status.OK;
        }
      } catch (Exception e) {
        // Try again
      }
    }
    System.err.println("Not possible to write the object "+key);
    return Status.ERROR;
  }
}
