package com.github.ldaniels528.qwery.etl.actors

import java.util.Date

import play.api.libs.json.Json

/**
  * Represents Job Statistics
  * @author lawrence.daniels@gmail.com
  */
case class JobStatistics(cpuLoad: Option[Double],
                         totalInserted: Option[Long],
                         bytesRead: Option[Long],
                         bytesPerSecond: Option[Double],
                         recordsDelta: Option[Long],
                         recordsPerSecond: Option[Double],
                         pctComplete: Option[Double],
                         completionTime: Option[Date])

/**
  * Job Statistics Companion
  * @author lawrence.daniels@gmail.com
  */
object JobStatistics {

  implicit val jobStatisticsFormat = Json.format[JobStatistics]

}
