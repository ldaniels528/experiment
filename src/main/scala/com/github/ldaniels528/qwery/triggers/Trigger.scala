package com.github.ldaniels528.qwery.triggers

import scala.concurrent.ExecutionContext

/**
  * Represents an event-based notification trigger
  * @author lawrence.daniels@gmail.com
  */
trait Trigger {

  def start()(implicit ec: ExecutionContext): Unit

  def stop(): Unit

}
