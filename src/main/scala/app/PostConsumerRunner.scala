package app

import app.kafka.{Consumer, ConsumerImpl, ConsumerRunner, ConsumerServer, ConsumerServerFactory}
import example.avro.messages.Post

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends ConsumerRunner[String, Post] {

  override val topic: String = "posts"

  override def subscribe(records: Iterator[(String, Post)]): Unit = {
    records.foreach { case (key: String, post: Post) =>
      log.info(s"key:$key, value: {id:${post.getId}, timestamp: ${post.getTimestamp}}")
    }
  }

}

class PostConsumerServerFactory extends ConsumerServerFactory {

  val GROUP_ID = "PostService"

  def consumer = new ConsumerImpl[String, Post](GROUP_ID)

  override def generate(): ConsumerServer = new PostConsumerRunner(consumer)

}