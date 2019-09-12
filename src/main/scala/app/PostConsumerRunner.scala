package app

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import app.kafka._
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Await
import scala.concurrent.duration._

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends ConsumerRunner[String, Post] {

  override val topic: String = "posts"

  val system: ActorSystem = ActorSystem("postConsumer")
  val postWriter: ActorRef = system.actorOf(Props(new PostWriter))

  implicit val timeout: Timeout = Timeout(5 seconds)

  override def subscribe(records: Iterator[ConsumerRecord[String, Post]]): Unit = {
    records.foreach { record =>
      val future = postWriter ? PostWriter.Write(record)
      Await.ready(future, 60 seconds)
      commit(record)
    }
  }

}

class PostConsumerServerFactory extends ConsumerServerFactory {

  val GROUP_ID = "PostService"

  def consumer = new ConsumerImpl[String, Post](GROUP_ID)

  override def generate(): ConsumerServer = new PostConsumerRunner(consumer)

}