package app.kafka

import akka.Done

import scala.concurrent.{ExecutionContext, Future}

trait ConsumerServer extends FutureRunnable {

  def onStart(): Unit

  def onStop(): Unit

  def onClose(): Unit

  def onError(e: Throwable): Unit

}

trait FutureRunnable {

  def run()(implicit ec: ExecutionContext): Future[Done]

}

trait ConsumerServerFactory {

  def generate(): ConsumerServer

}