package app

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import app.kafka._
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

object PostConsumerRunner {

  case class Run()

  case class Subscribe(record: ConsumerRecord[String, Post])

  case class Terminate()

}

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends ConsumerRunner[String, Post] with Actor {

  import PostConsumerRunner._

  override val topic: String = "posts"

  val postWriter: ActorRef = context.actorOf(Props[PostWriter])

  implicit val timeout: Timeout = Timeout(5 seconds)
  implicit val exec = context.dispatcher

  override def subscribe(records: Iterator[ConsumerRecord[String, Post]]): Unit = {
    records.foreach { record => self ! Subscribe(record) }
  }

  override def receive: Receive = start

  def start: Receive = {
    case Run() =>
      log.info("runn")
      run()
      context.become(running())
  }

  def running(): Receive = {
    case Subscribe(record: ConsumerRecord[String, Post]) =>
      postWriter ! PostWriter.Write(record)
      commit(record)
    case Terminate =>
      onStop()
      context.stop(self)
  }

}

class PostConsumerServerFactory extends ConsumerServerFactory {

  val GROUP_ID = "PostService"

  def consumer = new ConsumerImpl[String, Post](GROUP_ID)

  override def generate(): ConsumerServer = new PostConsumerRunner(consumer)

}