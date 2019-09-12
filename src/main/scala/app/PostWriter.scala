package app

import akka.actor.Actor
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

object PostWriter {

  case class Done(record: ConsumerRecord[String, Post])

  case class Write(record: ConsumerRecord[String, Post])

}

class PostWriter extends Actor with Logging {

  import PostWriter._

  override def receive: Receive = {
    case Write(record) =>
      log.info(s"key:${record.key()}, value: {id:${record.value().getId}, timestamp: ${record.value().getTimestamp}}")
      sender() ! Done(record)
  }

}