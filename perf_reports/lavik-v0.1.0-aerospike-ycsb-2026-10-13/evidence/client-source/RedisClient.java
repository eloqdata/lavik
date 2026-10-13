/**
 * Copyright (c) 2012-2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to Redis hashes. For scanning operations, keys
 * can be saved (by an arbitrary hash) in a sorted set. The index can be
 * disabled for workloads that do not scan, avoiding an extra write and a
 * global hot key.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.commands.BasicCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;
  private boolean scanIndexEnabled = true;
  private boolean replaceOnly;
  private static final byte[] HREPLACE_BYTES =
      "LAVIK.HREPLACE".getBytes(StandardCharsets.US_ASCII);
  private static final ProtocolCommand HREPLACE = () -> HREPLACE_BYTES;
  /** Experimental Keylane extension; does not change INSERT or scan indexing. */
  public static final String UPDATE_COMMAND_PROPERTY = "redis.updatecommand";

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  /** Selects whether the binding maintains the sorted-set scan index. */
  public static final String SCAN_INDEX_PROPERTY = "redis.scanindex";
  /** Preserves the historical binding behavior, including scan support. */
  public static final String SCAN_INDEX_ZSET = "zset";
  /** Skips index maintenance and makes scan operations unsupported. */
  public static final String SCAN_INDEX_NONE = "none";
  /** Keeps existing users compatible unless they explicitly opt out. */
  public static final String SCAN_INDEX_PROPERTY_DEFAULT = SCAN_INDEX_ZSET;

  public static final String INDEX_KEY = "_indices";

  /** Creates a Redis binding that YCSB will initialize from its properties. */
  public RedisClient() {
  }

  RedisClient(JedisCommands jedis, Properties props) throws DBException {
    this.jedis = jedis;
    configureScanIndex(props);
  }

  private void configureScanIndex(Properties props) throws DBException {
    String updateCommand = props.getProperty(UPDATE_COMMAND_PROPERTY, "hmset")
        .trim().toLowerCase(Locale.ROOT);
    if (!"hmset".equals(updateCommand) && !"lavik.hreplace".equals(updateCommand)) {
      throw new DBException("Invalid redis.updatecommand: " + updateCommand);
    }
    replaceOnly = "lavik.hreplace".equals(updateCommand);
    if (Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY, "false"))) {
      throw new DBException("Experimental Keylane binding requires redis.cluster=false");
    }
    if (replaceOnly && !Boolean.parseBoolean(props.getProperty("writeallfields", "false"))) {
      throw new DBException("LAVIK.HREPLACE binding requires writeallfields=true");
    }
    String scanIndex = props.getProperty(SCAN_INDEX_PROPERTY,
        SCAN_INDEX_PROPERTY_DEFAULT).trim().toLowerCase(Locale.ROOT);
    if (SCAN_INDEX_ZSET.equals(scanIndex)) {
      scanIndexEnabled = true;
    } else if (SCAN_INDEX_NONE.equals(scanIndex)) {
      scanIndexEnabled = false;
    } else {
      throw new DBException("Invalid " + SCAN_INDEX_PROPERTY + ": "
          + scanIndex + "; expected " + SCAN_INDEX_ZSET + " or "
          + SCAN_INDEX_NONE);
    }
  }

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    configureScanIndex(props);

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
    if (redisTimeout != null) {
      jedis = new Jedis(host, port, Integer.parseInt(redisTimeout));
    } else {
      jedis = new Jedis(host, port);
    }
    ((Jedis) jedis).connect();

    String password = props.getProperty(PASSWORD_PROPERTY);
    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  public void cleanup() throws DBException {
    try {
      ((Closeable) jedis).close();
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);
      List<String> values = jedis.hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK")) {
      if (scanIndexEnabled) {
        jedis.zadd(INDEX_KEY, hash(key), key);
      }
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (!scanIndexEnabled) {
      return jedis.del(key) == 0 ? Status.ERROR : Status.OK;
    }
    return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
        : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    if (replaceOnly) {
      Map<String, String> fields = StringByteIterator.getStringMap(values);
      String[] args = new String[1 + fields.size() * 2];
      args[0] = key;
      int position = 1;
      for (Map.Entry<String, String> field : fields.entrySet()) {
        args[position++] = field.getKey();
        args[position++] = field.getValue();
      }
      Jedis connection = (Jedis) jedis;
      connection.getClient().sendCommand(HREPLACE, args);
      String reply = connection.getClient().getStatusCodeReply();
      return reply == null ? Status.NOT_FOUND : "OK".equals(reply) ? Status.OK : Status.ERROR;
    }
    return jedis.hmset(key, StringByteIterator.getStringMap(values))
        .equals("OK") ? Status.OK : Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    if (!scanIndexEnabled) {
      return Status.NOT_IMPLEMENTED;
    }
    Set<String> keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
        Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
