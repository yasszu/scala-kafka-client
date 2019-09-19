package app

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Terminated}
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