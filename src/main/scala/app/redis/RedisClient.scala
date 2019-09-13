package app.redis

import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

trait RedisClient {

  def zadd(key: String, score: Double, member: String): Unit

  def zremRangeByRank(key: String, start: Int, stop: Int): Long

  def zrange(key: String, start: Int, stop: Int): Seq[String]

  def ping(): String

}

object RedisClient {
  def apply(): RedisClient = new RedisClientImpl(RedisConnectionPoolImpl)
}

class RedisClientImpl(pool: RedisConnectionPool) extends RedisClient {

  private def request[T](command: Jedis => T): T = {
    var client: Jedis = null
    try {
      client = pool.getClient
      command(client)
    } finally {
      if (client != null) {
        client.close()
      }
    }
  }

  override def zadd(key: String, score: Double, member: String): Unit = {
    request { client =>
      client.zadd(key, score, member)
    }
  }


  override def zremRangeByRank(key: String, start: Int, stop: Int): Long = {
    request { client =>
      client.zremrangeByRank(key, start, stop)
    }
  }

  override def zrange(key: String, start: Int, stop: Int): Seq[String] = {
    request { client =>
      val elements = client.zrange(key, start, stop)
      elements.asScala.toSeq
    }
  }

  override def ping(): String = {
    request { client =>
      client.ping()
    }
  }
}
