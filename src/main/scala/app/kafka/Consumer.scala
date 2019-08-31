package app.kafka

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

trait Consumer[K, V] {

  def subscribe(topic: String): Unit

  def poll(timeout: Long): Iterator[(K, V)]

  def commit(): Unit

  def wakeup(): Unit

  def close(): Unit

}

class ConsumerImpl[K, V](groupId: String, props: Map[String, String] = Map.empty) extends Consumer[K, V] {
  self =>

  val GROUP_ID = "group.id"

  lazy val config: Config = ConfigFactory.load().getConfig("kafka.consumer")
  lazy val bootstrapServer: String = config.getString("bootstrap.servers")
  lazy val enableAutoCommit: String = config.getString("enable.auto.commit")
  lazy val autoCommitIntervalMs: String = config.getString("auto.commit.interval.ms")
  lazy val schemaRegistryUrl: String = config.getString("schema.registry.url")
  lazy val avroDeserializer: String = config.getString("avro.deserializer")
  lazy val keyDeserializer: String = config.getString("key.deserializer")
  lazy val valueDeserializer: String = config.getString("value.deserializer")

  lazy val groupIdProp: Map[String, String] = Map(GROUP_ID -> groupId)

  lazy val consumer = new KafkaConsumer[K, V](buildProps)

  private def buildProps: Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", bootstrapServer)
    p.setProperty("enable.auto.commit", enableAutoCommit)
    p.setProperty("auto.commit.interval.ms", autoCommitIntervalMs)
    p.setProperty("schema.registry.url", schemaRegistryUrl)
    p.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, avroDeserializer)
    p.setProperty("key.deserializer", keyDeserializer)
    p.setProperty("value.deserializer", valueDeserializer)
    setProps(p, groupIdProp)
    setProps(p, props)
    p
  }

  private def setProps(properties: Properties, props: Map[String, String]): Unit = {
    props.foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
  }

  override def subscribe(topic: String): Unit = {
    consumer.subscribe(java.util.Arrays.asList(topic))
  }

  override def poll(timeout: Long): Iterator[(K, V)] = {
    val records = consumer.poll(timeout).iterator().asScala
    records.map { record => (record.key(), record.value()) }
  }


  override def commit(): Unit = consumer.commitAsync()

  override def wakeup(): Unit = consumer.wakeup()

  override def close(): Unit = consumer.close()

}