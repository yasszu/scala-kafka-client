package app

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.util.Timeout
import akka.pattern.ask
import app.kafka.{Consumer, ConsumerImpl, ConsumerRunner, ConsumerServer, ConsumerServerFactory}
import example.avro.messages.Post

import scala.concurrent.Await
import scala.concurrent.duration._

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends ConsumerRunner[String, Post] {

  override val topic: String = "posts"

  val system: ActorSystem = ActorSystem("postConsumer")
  val postWriter: ActorRef = system.actorOf(Props(new PostWriter))

  implicit val timeout: Timeout = Timeout(5 seconds)

  override def subscribe(records: Iterator[(String, Post)]): Unit = {
    records.foreach { case (key: String, post: Post) =>
      val future = postWriter ? PostWriter.Write(key, post)
      Await.ready(future, 60 seconds)
      commit()
    }
  }

}

class PostConsumerServerFactory extends ConsumerServerFactory {

  val GROUP_ID = "PostService"

  def consumer = new ConsumerImpl[String, Post](GROUP_ID)

  override def generate(): ConsumerServer = new PostConsumerRunner(consumer)

}