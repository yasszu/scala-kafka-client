package app

import akka.actor.ActorSystem
import app.kafka.ConsumerManager
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext

object Main extends App {
  self =>

  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  implicit val system: ActorSystem = ActorSystem("kafka")
  implicit val ec: ExecutionContext = system.dispatcher

  val postProducerServer = PostProducerServer()
  val consumerManger = ConsumerManager()

  // Start a producer
  postProducerServer.run()

  // Start a consumer
  consumerManger.addFactory(PostConsumerServerFactory)
  consumerManger.runAll()

  // Stop the consumer when the VM exits
  sys.addShutdownHook {
    logger.info("Stopping consumer...")
    consumerManger.shutdown()
    system.terminate()
  }

}