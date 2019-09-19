package app

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import app.kafka.{ConsumerImpl, ConsumerManager}
import app.redis.JedisConnectionPool
import app.util.Logging
import PostConsumerRunner._
import example.avro.messages.Post

import scala.concurrent.ExecutionContext

case class ShutDown()

class ActorMain extends Actor with Logging {

  implicit val system: ActorSystem = context.system
  implicit val ec: ExecutionContext = context.dispatcher

//  implicit val kafkaSystem: ActorSystem = ActorSystem("kafka-consumer")
//  implicit val kafkaExec: ExecutionContext = kafkaSystem.dispatcher

  val postProducerServer = PostProducerServer()

  val postConsumerRunner: ActorRef = {
    val consumer = new ConsumerImpl[String, Post]("PostService")
    context.actorOf(Props(classOf[PostConsumerRunner], consumer))
  }

  // Init
  JedisConnectionPool.init()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  postConsumerRunner ! PostConsumerRunner.Run()

  // Stop the consumer when the VM exits
  sys.addShutdownHook {
    self ! ShutDown()
  }

  override def receive: Receive = {
    case ShutDown =>
      log.info("Stopping consumer...")
      JedisConnectionPool.close()
      postConsumerRunner ! PostConsumerRunner.Terminate()
      context.stop(self)
  }

}