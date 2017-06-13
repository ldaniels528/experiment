package com.github.ldaniels528.transgress.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Statistics
  */
@ScalaJSDefined
class StatisticsLike(val cpuLoad: Double,
                     val totalInserted: Double,
                     val bytesRead: Double,
                     val bytesPerSecond: Double,
                     val recordsDelta: Double,
                     val recordsPerSecond: Double,
                     val pctComplete: Double,
                     val completionTime: Double) extends js.Object