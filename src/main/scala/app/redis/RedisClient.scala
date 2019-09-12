package app.redis

import app.util.Logging
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

trait RedisClient {

  def zadd(key: String, value: String, score: Double): Unit

  def ping(): String

}

object RedisClient {
  def apply(): RedisClient = new RedisClientImpl()
}

class RedisClientImpl extends RedisClient {

  def getClient: Jedis = RedisConnectionPool.getClient

  private def write(command: Jedis => Long): Long = {
    var client: Jedis = null
    try {
      client = getClient
      command(client)
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  private def read(command: Jedis => String): String = {
    var client: Jedis = null
    try {
      client = getClient
      command(client)
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }


  override def zadd(key: String, value: String, score: Double): Unit = {
    write { client => client.zadd(key, score, value) }
  }

  override def ping(): String = {
    read { client => client.ping() }
  }
}

object RedisConnectionPool extends Logging {

  private lazy val pool = new JedisPool(getConfig, "localhost", 6379)

  private def getConfig = new JedisPoolConfig

  def init(): Unit = {
    var client: Jedis = null
    try {
      client = pool.getResource
      client.ping()
      log.info("Redis Connection Pooling is successfully started.")
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  def getClient: Jedis = pool.getResource

  def close(): Unit = pool.close()

}

