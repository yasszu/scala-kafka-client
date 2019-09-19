package app

import akka.actor.ActorSystem
import app.kafka.ConsumerManager
import app.redis.JedisConnectionPool
import app.util.Logging

import scala.concurrent.ExecutionContext

object Main extends App with Logging {
  self =>

  implicit val system: ActorSystem = ActorSystem("kafka")
  implicit val ec: ExecutionContext = system.dispatcher

  val postProducerServer = PostProducerServer()
  val consumerManger = ConsumerManager()

  // Init
  JedisConnectionPool.init()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  consumerManger.addFactory(new PostConsumerServerFactory())
  consumerManger.runAll()

  // Stop the consumer when the VM exits
  sys.addShutdownHook {
    log.info("Stopping consumer...")
    consumerManger.shutdown()
    JedisConnectionPool.close()
    system.terminate()
  }

}