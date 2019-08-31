package app.kafka

import app.util.{Delay, Logging}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}

class ConsumerManager extends Delay with Logging {
  self =>

  private var servers: Map[Int, ConsumerServer] = Map.empty

  private var factories: Map[Int, ConsumerServerFactory] = Map.empty

  private val DELAY_FOR_RETRY = 15 seconds

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
        log.info("Stopped successfully")
      case Failure(_) =>
        log.error("Non fatal error occurred")
        resetServer(pid)
        log.error("Waiting for restating a consumer")
        delay(DELAY_FOR_RETRY) {
          log.info("Restart consumer")
          run(pid)
        }
    }
  }

  private def resetServer(pid: Int): Unit = {
    log.info("Reset consumer server")
    servers -= pid
    val server = factories(pid).generate()
    servers += (pid -> server)
  }

  def shutdown(): Unit = {
    log.info("Shutdown consumers")
    servers.values.foreach(_.onStop())
  }

}

object ConsumerManager {

  def apply(): ConsumerManager = new ConsumerManager()

}