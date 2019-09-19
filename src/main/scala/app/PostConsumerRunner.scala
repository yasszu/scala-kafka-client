package app

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import app.kafka._
import app.util.Logging
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object PostConsumerRunner {

  case class Run()

  case class Poll(timeout: Int = 100)

  case class Subscribe(record: ConsumerRecord[String, Post])

  case class Terminate()

}

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends Actor with Logging {

  import PostConsumerRunner._

  val topic: String = "posts"

  val pollTimeout: FiniteDuration = 1000 milliseconds

  implicit val timeout: Timeout = Timeout(5 seconds)
  implicit val exec = context.dispatcher

  val postWriter: ActorRef = context.actorOf(Props[PostWriter])

  override def receive: Receive = waiting

  def waiting: Receive = {
    case Run() =>
      consumer.subscribe(topic)
      self ! Poll()
      context.become(running)
  }

  def running: Receive = {
    case Poll(timeout) =>
      val records = consumer.poll(timeout)
      records.foreach { record => self ! Subscribe(record) }
      self ! Poll()
    case Subscribe(record: ConsumerRecord[String, Post]) =>
      postWriter ! PostWriter.Write(record)
      consumer.commit(record)
    case Terminate =>
      consumer.close()
      context.stop(self)
  }

}
