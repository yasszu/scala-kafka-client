package app.util

import org.slf4j.{LoggerFactory, Logger => SLF4Logger}

trait Logging {
  self =>

  val log: Logger = new LoggerImpl(self.getClass)

}

trait Logger {

  def debug(msg: String): Unit

  def info(msg: String): Unit

  def error(msg: String): Unit

  def warn(msg: String): Unit

}

class LoggerImpl(clazz: Class[_]) extends Logger {

  private val logger: SLF4Logger = LoggerFactory.getLogger(clazz)

  def debug(msg: String): Unit = logger.debug(msg)

  def info(msg: String): Unit = logger.info(msg)

  def error(msg: String): Unit = logger.error(msg)

  def warn(msg: String): Unit = logger.warn(msg)

}