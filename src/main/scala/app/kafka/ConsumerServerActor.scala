package app.kafka

import akka.actor.{Actor, OneForOneStrategy, SupervisorStrategy, Terminated}
import akka.util.Timeout
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.language.postfixOps

object ConsumerServerActor {

  case class Run()

}

trait ConsumerServerActor[K, V] extends Actor with ConsumerServer[K, V] with Logging {

  import ConsumerServerActor._

  case class Poll(timeout: FiniteDuration)

  case class Subscribe(record: ConsumerRecord[K, V])

  implicit val timeout: Timeout = Timeout(5 seconds)
  implicit val exec: ExecutionContextExecutor = context.dispatcher

  val pollTimeout: FiniteDuration = 1000 milliseconds

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Run() =>
      onStart()
      self ! Poll(pollTimeout)
      context.become(running)
  }

  def running: Receive = {
    case Poll(timeout) =>
      val records = poll(timeout)
      records.foreach { record => self ! Subscribe(record) }
      self ! Poll(pollTimeout)
    case Subscribe(record: ConsumerRecord[String, Post]) =>
      subscribe(record)
    case Terminated(_) =>
      onClose()
      context.stop(self)
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
    case e: Exception =>
      onError(e)
      SupervisorStrategy.restart
  }

}
