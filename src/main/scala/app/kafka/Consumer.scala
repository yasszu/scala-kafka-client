package app.kafka

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

trait Consumer[K, V] {

  def subscribe(topic: String): Unit

  def poll(timeout: Long): Iterator[(K, V)]

  def wakeup(): Unit

  def close(): Unit

}

object Consumer {

  val GROUP_ID = "group.id"

  def groupIdProp(groupId: String): Map[String, String] = Map(GROUP_ID -> groupId)

  def apply[K, V](groupId: String, props: Map[String, String]): Consumer[K, V] = new ConsumerImpl[K, V](props ++ groupIdProp(groupId))

  def apply[K, V](groupId: String): Consumer[K, V] = new ConsumerImpl[K, V](groupIdProp(groupId))

}

class ConsumerImpl[K, V](props: Map[String, String]) extends Consumer[K, V] {

  lazy val config: Config = ConfigFactory.load().getConfig("kafka.consumer")
  lazy val bootstrapServer: String = config.getString("bootstrap.servers")
  lazy val enableAutoCommit: String = config.getString("enable.auto.commit")
  lazy val autoCommitIntervalMs: String = config.getString("auto.commit.interval.ms")
  lazy val schemaRegistryUrl: String = config.getString("schema.registry.url")
  lazy val avroDeserializer: String = config.getString("avro.deserializer")
  lazy val keyDeserializer: String = config.getString("key.deserializer")
  lazy val valueDeserializer: String = config.getString("value.deserializer")

  private def buildProps: Properties = {
    val p = new Properties()
    p.setProperty("bootstrap.servers", bootstrapServer)
    p.setProperty("enable.auto.commit", enableAutoCommit)
    p.setProperty("auto.commit.interval.ms", autoCommitIntervalMs)
    p.setProperty("schema.registry.url", schemaRegistryUrl)
    p.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, avroDeserializer)
    p.setProperty("key.deserializer", keyDeserializer)
    p.setProperty("value.deserializer", valueDeserializer)
    setCustomProps(p)
    p
  }

  private def setCustomProps(properties: Properties): Unit = {
    props.foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
  }

  private val consumer = new KafkaConsumer[K, V](buildProps)

  override def subscribe(topic: String): Unit = {
    consumer.subscribe(java.util.Arrays.asList(topic))
  }

  override def poll(timeout: Long): Iterator[(K, V)] = {
    val records = consumer.poll(timeout).iterator().asScala
    records.map { record => (record.key(), record.value()) }
  }

  override def wakeup(): Unit = consumer.wakeup()

  override def close(): Unit = consumer.close()

}