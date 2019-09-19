package app

import java.sql.Timestamp

import akka.actor.Actor
import app.redis.{RedisClient, RedisModule}
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.google.inject.Guice

object PostWriter {

  case class Done(record: ConsumerRecord[String, Post])

  case class Write(record: ConsumerRecord[String, Post])

}

class PostWriter extends Actor with Logging {

  import PostWriter._
  import net.codingwell.scalaguice.InjectorExtensions._

  val injector = Guice.createInjector(new RedisModule())
  val redis = injector.instance[RedisClient]

  override def receive: Receive = {
    case Write(record) =>
      val date = "%tY-%<tm-%<td %<tH:%<tM:%<tS" format new Timestamp(record.value().getTimestamp)
      log.info(s"key:${record.key()}, value: {id:${record.value().getId}, date: $date}")
      saveRecord(record.value())
      sender() ! Done(record)
  }

  def saveRecord(post: Post): Long = {
    val key = "post:test"
    val score = post.getTimestamp
    val member = s"${post.getId}"
    redis.zadd(key, score, member)
    redis.zremRangeByRank(key, 0, -51)
  }

}