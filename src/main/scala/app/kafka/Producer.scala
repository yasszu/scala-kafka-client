package app.kafka

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait Producer[K, V] {

  def send(record: ProducerRecord[K, V]): Unit

}

object Producer {

  def apply[K, V](): Producer[K, V] = new ProducerImpl(Map.empty)

}

class ProducerImpl[K, V](props: Map[String, String]) extends Producer[K, V] {

  lazy val config: Config = ConfigFactory.load().getConfig("kafka.producer")
  lazy val bootstrapServer: String = config.getString("bootstrap.servers")
  lazy val acks: String = config.getString("acks")
  lazy val schemaRegistryUrl: String = config.getString("schema.registry.url")
  lazy val keySerializer: String = config.getString("key.serializer")
  lazy val valueSerializer: String = config.getString("value.serializer")

  val producer = new KafkaProducer[K, V](buildProps(props))

  def buildProps(props: Map[String, String]): Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", bootstrapServer)
    p.setProperty("acks", acks)
    p.setProperty("schema.registry.url", schemaRegistryUrl)
    p.setProperty("key.serializer", keySerializer)
    p.setProperty("value.serializer", valueSerializer)
    setCustomProps(p)
    p
  }

  private def setCustomProps(properties: Properties): Unit = {
    props.foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
  }

  override def send(record: ProducerRecord[K, V]): Unit = producer.send(record)

}
