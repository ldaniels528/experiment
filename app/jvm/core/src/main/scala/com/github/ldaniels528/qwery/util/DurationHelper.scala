package com.github.ldaniels528.qwery.util

import scala.concurrent.duration._

/**
  * Duration Utilities
  * @author lawrence.daniels@gmail.com
  */
object DurationHelper {

  implicit def duration2Long(duration: Duration): Long = duration.toMillis

  implicit def long2duration(value: Long): FiniteDuration = value.millis

}
