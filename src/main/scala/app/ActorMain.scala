package app

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.pipe
import app.kafka.ConsumerManager
import app.redis.JedisConnectionPool
import app.util.Logging

import scala.concurrent.{ExecutionContext, Promise}

case class Shutdown()

class ActorMain extends Actor with Logging {

  implicit val exec: ExecutionContext = context.dispatcher
  implicit val system = context.system

  val postStreamConsumer = context.actorOf(Props[PostStreamConsumer], "postStreamConsumer")
  val postProducerServer = PostProducerServer()
  val consumerManger = ConsumerManager()

  // Init
  JedisConnectionPool.init()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  postStreamConsumer ! PostStreamConsumer.Run

  // Stop the consumer when the VM exits
  val promise = Promise[Shutdown]()
  sys.addShutdownHook {
    promise success Shutdown()
  }

  promise.future pipeTo self

  override def receive: Receive = {
    case Shutdown =>
      log.info("Stopping consumer...")
      consumerManger.shutdown()
      JedisConnectionPool.close()
      system.terminate()
  }

}