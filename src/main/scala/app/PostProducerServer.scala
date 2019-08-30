package app

import java.util.Properties

import akka.actor.{ActorSystem, Cancellable}
import com.typesafe.config.{Config, ConfigFactory}
import example.avro.messages.Post
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps

class PostProducerServer { self =>

  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  val topic: String = "post"

  lazy val config: Config = ConfigFactory.load().getConfig("kafka.producer")
  lazy val bootstrapServer: String = config.getString("bootstrap.servers")
  lazy val acks: String = config.getString("acks")
  lazy val schemaRegistryUrl: String = config.getString("schema.registry.url")
  lazy val keySerializer: String = config.getString("key.serializer")
  lazy val valueSerializer: String = config.getString("value.serializer")

  val props: Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", bootstrapServer)
    p.setProperty("acks", acks)
    p.setProperty("schema.registry.url", schemaRegistryUrl)
    p.setProperty("key.serializer", keySerializer)
    p.setProperty("value.serializer", valueSerializer)
    p
  }

  val producer = new KafkaProducer[String, Post](props)

  def run()(implicit ec: ExecutionContext, system: ActorSystem): Cancellable = {
    logger.info("Start a producer")
    system.scheduler.schedule(1 seconds, 1 seconds) {
      logger.info("Send messages")
      sendRecords(5)
    }
  }

  def sendRecords(amount: Int): Unit = {
    (1 to amount).map { v =>
      val timestamp = System.currentTimeMillis()
      val post = new Post()
      post.setId(v)
      post.setTimestamp(timestamp)
      val record = createRecord("none", post)
      producer.send(record)
    }
  }

  def createRecord(key: String, value: Post): ProducerRecord[String, Post] = {
    new ProducerRecord[String, Post](topic, key, value)
  }

}


object PostProducerServer {
  def apply(): PostProducerServer = new PostProducerServer()
}