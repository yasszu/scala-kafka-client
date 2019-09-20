package app

import akka.actor.{ActorRef, OneForOneStrategy, Props, SupervisorStrategy}
import app.kafka._
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.language.postfixOps

object PostConsumerServer {

  val topic: String = "posts"

  val groupId = "PostService"

  def props(): Props = {
    val consumer = new ConsumerImpl[String, Post](groupId)
    Props(new PostConsumerServer(topic, consumer))
  }

}

class PostConsumerServer(topic: String, consumer: Consumer[String, Post]) extends ConsumerServerActor[String, Post] {

  val postWriter: ActorRef = context.actorOf(Props[PostWriter])

  override def onStart(): Unit = {
    consumer.subscribe(topic)
  }

  override def subscribe(record: ConsumerRecord[String, Post]): Unit = {
    postWriter ! PostWriter.Write(record)
    consumer.commit(record)
  }

  override def poll(timeout: FiniteDuration): Iterator[ConsumerRecord[String, Post]] = {
    consumer.poll(timeout.toMillis)
  }

  override def onClose(): Unit = {
    consumer.close()
  }

  override def onError(e: Exception): Unit = {
    e.printStackTrace()
  }

}