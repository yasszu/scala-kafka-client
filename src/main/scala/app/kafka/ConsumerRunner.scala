package app.kafka

import akka.Done
import app.util.Logging
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait ConsumerRunner[K, V] extends ConsumerServer with Logging {
  self =>

  val pollTimeout: FiniteDuration = 1000 milliseconds

  val topic: String

  val consumer: Consumer[K, V]

  override def run()(implicit ec: ExecutionContext): Future[Done] = {
    // Setup the consumer
    onStart()

    // Start subscribing messages
    val promise = Promise[Done]()
    Future {
      val done = Try {
        while (true) {
          subscribe(consumer.poll(pollTimeout.toMillis))
        }
        Done
      } recoverWith {
        case _: WakeupException => Success(Done)
        case e =>
          onError(e)
          handleNonFatalError(e)
      }
      done match {
        case Success(done) =>
          onClose()
          promise success done
        case Failure(e) =>
          onClose()
          promise failure e
      }
    }
    promise.future
  }

  def subscribe(records: Iterator[ConsumerRecord[K, V]]): Unit

  def commit(): Unit = {
    consumer.commitAsync()
  }

  def commit(record: ConsumerRecord[K, V]): Unit = {
    val topicPartition = new TopicPartition(record.topic(), record.partition())
    val offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1)
    val currentOffset = Map(topicPartition -> offsetAndMetadata)
    consumer.commitAsync(currentOffset)
  }

  def handleNonFatalError(error: Throwable): Try[Done] = {
    Failure(error)
  }

  override def onStart(): Unit = {
    log.info("Start a consumer")
    consumer.subscribe(topic)
  }

  override def onStop(): Unit = {
    log.info("Stop a consumer")
    consumer.wakeup()
  }

  override def onClose(): Unit = {
    log.info("Close a consumer")
    consumer.close()
  }


  override def onError(e: Throwable): Unit = {
    log.error("Non fatal error occurred")
    e.printStackTrace()
  }

}
