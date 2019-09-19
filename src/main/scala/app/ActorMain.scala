package app

import akka.actor.{Actor, Props, Terminated}
import app.redis.JedisConnectionPool
import app.util.Logging

import scala.concurrent.ExecutionContext

case class Shutdown()

class ActorMain extends Actor with Logging {

  implicit val exec: ExecutionContext = context.dispatcher
  implicit val system = context.system

  val postProducerServer = PostProducerServer()

  val postStreamConsumer = context.actorOf(Props[PostStreamConsumerService])
  context.watch(postStreamConsumer)

  // Init
  JedisConnectionPool.init()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  postStreamConsumer ! PostStreamConsumerService.Run()

  override def receive: Receive = {
    case Terminated(_)  => context.stop(self)
  }

  override def postStop(): Unit = {
    log.info("Stopping consumer...")
    JedisConnectionPool.close()
    context.stop(self)
  }

}