package app

import akka.actor.{Actor, ActorRef, ActorSystem, Terminated}
import app.redis.JedisConnectionPool
import app.util.Logging

import scala.concurrent.ExecutionContext

case class ShutDown()

class ActorMain extends Actor with Logging {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher

  val postProducerServer = PostProducerServer()

  val postConsumerRunner: ActorRef = context.actorOf(PostConsumerRunner.props())
  context.watch(postConsumerRunner)

  // Init
  JedisConnectionPool.init()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  postConsumerRunner ! PostConsumerRunner.Run()

  override def receive: Receive = {
    case Terminated(_)  => context.stop(self)
  }

  override def postStop(): Unit = {
    log.info("Stopping consumer...")
    JedisConnectionPool.close()
    context.stop(self)
  }

}