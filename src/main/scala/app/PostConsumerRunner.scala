package app

import app.kafka.{Consumer, ConsumerRunner, ConsumerServer, ConsumerServerFactory}
import example.avro.messages.Post

class PostConsumerRunner(val consumer: Consumer[String, Post]) extends ConsumerRunner[String, Post] {

  override val topic: String = "posts"

  override def subscribe(records: Iterator[(String, Post)]): Unit = {
    records.foreach { case (key: String, post: Post) =>
      logger.info(s"key:$key, value: {id:${post.getId}, timestamp: ${post.getTimestamp}}")
    }
  }

}

object PostConsumerServerFactory extends ConsumerServerFactory {

  def createConsumer(groupId: String): Consumer[String, Post] = Consumer(groupId)

  override def generate(): ConsumerServer = new PostConsumerRunner(createConsumer("PostService"))

}