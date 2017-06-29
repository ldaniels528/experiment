package com.github.ldaniels528.transgress

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Remote Event
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class RemoteEvent(val action: js.UndefOr[String], val data: js.UndefOr[String]) extends js.Object

/**
  * RemoteEvent Singleton
  * @author lawrence.daniels@gmail.com
  */
object RemoteEvent {
  val FEED_UPDATE = "FEED_UPDATE"
  val JOB_UPDATE = "JOB_UPDATE"
  val SLAVE_UPDATE = "SLAVE_UPDATE"

}