package app

import akka.actor.Actor
import app.util.Logging
import example.avro.messages.Post

object PostWriter {

  case class Done(key: String)

  case class Write(key: String, post: Post)

}

class PostWriter extends Actor with Logging {

  import PostWriter._

  override def receive: Receive = {
    case Write(key, post) =>
      log.info(s"key:$key, value: {id:${post.getId}, timestamp: ${post.getTimestamp}}")
      sender() ! Done(key)
  }

}