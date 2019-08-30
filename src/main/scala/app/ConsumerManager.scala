package app

import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class ConsumerManager extends Delay {
  self =>

  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  private var servers: Map[Int, ConsumerServer] = Map.empty

  private var factories: Map[Int, ConsumerServerFactory] = Map.empty

  def addFactory(factory: ConsumerServerFactory): Unit = {
    val pid = factories.size + 1
    factories += (pid -> factory)
  }

  def runAll()(implicit ec: ExecutionContext): Unit = {
    initServers()
    servers.keys.foreach(run)
  }

  private def initServers(): Unit = {
    factories.foreach { case (pid, factory) =>
      val server = factory.generate()
      servers += (pid -> server)
    }
  }

  private def run(pid: Int)(implicit ec: ExecutionContext): Unit = {
    servers(pid).run() onComplete {
      case Success(_) =>
        logger.info("Stopped successfully")
      case Failure(_) =>
        logger.error("Non fatal error occurred")
        resetServer(pid)
        logger.error("Waiting for restating a consumer")
        delay(15 seconds) {
          logger.info("Restart consumer")
          run(pid)
        }
    }
  }

  private def resetServer(pid: Int): Unit = {
    logger.info("Reset consumer server")
    servers -= pid
    val server = factories(pid).generate()
    servers += (pid -> server)
  }

  def shutdown(): Unit = {
    logger.info("Shutdown consumers")
    servers.values.foreach(_.onStop())
  }

}

object ConsumerManager {
  def apply(): ConsumerManager = new ConsumerManager()
}