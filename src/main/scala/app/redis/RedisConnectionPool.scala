package app.redis

import app.util.Logging
import com.typesafe.config.{Config, ConfigFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

trait RedisConnectionPool {
  def init(): Unit

  def close(): Unit

  def getClient: Jedis
}

object JedisConnectionPool extends RedisConnectionPool with Logging {

  lazy val config: Config = ConfigFactory.load().getConfig("redis")
  lazy val host: String = config.getString("host")
  lazy val port: Int = config.getInt("port")

  private lazy val pool = new JedisPool(getConfig, host, port)

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

