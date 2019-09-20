package app

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, SupervisorStrategy, Terminated}
import akka.util.Timeout
import app.kafka._
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.language.postfixOps

object PostConsumerRunner {

  case class Run()

  case class Poll(timeout: FiniteDuration)

  case class Subscribe(record: ConsumerRecord[String, Post])

  case class Terminate()

  val topic: String = "posts"

  val groupId = "PostService"

  def consumer = new ConsumerImpl[String, Post](groupId)

  def props(): Props = Props(new PostConsumerRunner(topic, consumer))

}

class PostConsumerRunner(topic: String, consumer: Consumer[String, Post]) extends Actor with Logging {

  import PostConsumerRunner._

  implicit val timeout: Timeout = Timeout(5 seconds)
  implicit val exec: ExecutionContextExecutor = context.dispatcher

  val pollTimeout: FiniteDuration = 1000 milliseconds

  val postWriter: ActorRef = context.actorOf(Props[PostWriter])

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Run() =>
      consumer.subscribe(topic)
      self ! Poll(pollTimeout)
      context.become(running)
  }

  def running: Receive = {
    case Poll(timeout) =>
      val records = consumer.poll(timeout.toMillis)
      records.foreach { record => self ! Subscribe(record) }
      self ! Poll(pollTimeout)
    case Subscribe(record: ConsumerRecord[String, Post]) =>
      postWriter ! PostWriter.Write(record)
      consumer.commit(record)
    case Terminated(_) =>
      consumer.close()
      context.stop(self)
  }

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
    case _: Exception => SupervisorStrategy.restart
  }

}
