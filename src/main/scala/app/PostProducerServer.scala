package app

import akka.actor.{ActorSystem, Cancellable}
import app.kafka.Producer
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class PostProducerServer extends Logging {
  self =>

  val topic: String = "posts"

  val producer: Producer[String, Post] = Producer()

  def run()(implicit ec: ExecutionContext, system: ActorSystem): Cancellable = {
    log.info("Start a producer")
    system.scheduler.schedule(1 seconds, 1 seconds) {
      log.info("Send messages")
      sendRecords(5)
    }
  }

  def sendRecords(amount: Int): Unit = {
    (1 to amount).foreach { v =>
      val id = scala.util.Random.nextInt(10000000)
      val timestamp = System.currentTimeMillis()
      val post = new Post()
      post.setId(id)
      post.setTimestamp(timestamp)
      val record = createRecord("none", post)
      producer.send(record)
    }
  }

  def createRecord(key: String, value: Post): ProducerRecord[String, Post] = new ProducerRecord(topic, key, value)

}

object PostProducerServer {

  def apply(): PostProducerServer = new PostProducerServer()

}