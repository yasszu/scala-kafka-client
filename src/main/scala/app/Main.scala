package app

import akka.actor.ActorSystem
import app.kafka.ConsumerManager
import app.util.Logging

import scala.concurrent.ExecutionContext

object Main extends App with Logging {
  self =>

  implicit val system: ActorSystem = ActorSystem("kafka")
  implicit val ec: ExecutionContext = system.dispatcher

  val postProducerServer = PostProducerServer()
  val consumerManger = ConsumerManager()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  consumerManger.addFactory(new PostConsumerServerFactory())
  consumerManger.runAll()

  // Stop the consumer when the VM exits
  sys.addShutdownHook {
    log.info("Stopping consumer...")
    consumerManger.shutdown()
    system.terminate()
  }

}