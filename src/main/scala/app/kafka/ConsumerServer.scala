package app.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration.FiniteDuration

trait ConsumerServer[K, V] {

  def onStart(): Unit

  def subscribe(record: ConsumerRecord[K, V]): Unit

  def poll(timeout: FiniteDuration): Iterator[ConsumerRecord[K, V]]

  def onClose(): Unit

  def onError(e: Exception)

}
