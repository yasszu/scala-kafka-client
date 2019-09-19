package app.redis

import com.google.inject.AbstractModule
import net.codingwell.scalaguice.ScalaModule

class RedisModule extends AbstractModule with ScalaModule {

  override def configure(): Unit = {
    bind[RedisConnectionPool].toInstance(JedisConnectionPool)
    bind[RedisClient].to[JedisClient]
  }

}
