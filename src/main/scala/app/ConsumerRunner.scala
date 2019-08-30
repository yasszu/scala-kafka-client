package app

import akka.Done
import org.apache.kafka.common.errors.WakeupException
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait ConsumerRunner[K, V] extends ConsumerServer {
  self =>

  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  val timeout: FiniteDuration = 1000 milliseconds

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
          subscribe(consumer.poll(timeout.toMillis))
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

  def handleNonFatalError(error: Throwable): Try[Done] = {
    Failure(error)
  }

  override def onStart(): Unit = {
    logger.info("Start a consumer")
    consumer.subscribe(topic)
  }

  override def onStop(): Unit = {
    logger.info("Stop a consumer")
    consumer.wakeup()
  }

  override def onClose(): Unit = {
    logger.info("Close a consumer")
    consumer.close()
  }


  override def onError(e: Throwable): Unit = {
    logger.error("Non fatal error occurred")
    e.printStackTrace()
  }

  def subscribe(records: Iterator[(K, V)]): Unit

}