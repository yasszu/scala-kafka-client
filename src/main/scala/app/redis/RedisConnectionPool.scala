package app.redis

import app.util.Logging
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

trait RedisConnectionPool {
  def init(): Unit

  def close(): Unit

  def getClient: Jedis
}

object RedisConnectionPoolImpl extends RedisConnectionPool with Logging {

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

