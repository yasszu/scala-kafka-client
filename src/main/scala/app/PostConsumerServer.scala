package app

import akka.actor.{ActorRef, Props}
import app.kafka._
import example.avro.messages.Post
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.duration._
import scala.language.postfixOps

object PostConsumerServer {

  val topic: String = "posts"

  val groupId = "PostService"

  def getConsumer: Consumer[String, Post] = new ConsumerImpl[String, Post](groupId)

  def props(): Props = Props(new PostConsumerServer(topic, getConsumer))

}

class PostConsumerServer(topic: String, consumer: Consumer[String, Post]) extends ConsumerServerActor[String, Post] {

  val postWriter: ActorRef = context.actorOf(PostWriter.props())

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