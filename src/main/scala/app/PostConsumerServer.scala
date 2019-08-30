package app

import example.avro.messages.Post

class PostConsumerServer() extends ConsumerServer[String, Post] {

  override val topic: String = "post"

  override val consumer: Consumer[String, Post] = ConsumerImpl[String, Post]("PostService")

  override def subscribe(records: Iterator[(String, Post)]): Unit = {
    records.foreach { case (key: String, post: Post) =>
      println(s"key:$key, value: {id:${post.getId}, timestamp: ${post.getTimestamp}}")
    }
  }

}

object PostConsumerServerFactory extends ConsumerServerFactory {

  override def generate(): ConsumerApplication = new PostConsumerServer()

}