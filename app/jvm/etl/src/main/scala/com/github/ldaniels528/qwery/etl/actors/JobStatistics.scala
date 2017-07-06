package com.github.ldaniels528.qwery.etl.actors

import java.util.Date

import com.github.ldaniels528.qwery.etl.QweryETL.getCpuLoad
import com.github.ldaniels528.qwery.sources.Statistics
import com.github.ldaniels528.qwery.util.OptionHelper.Risky._
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

  implicit def statisticsConversion(stats: Statistics): JobStatistics = JobStatistics(
    cpuLoad = None,
    totalInserted = stats.totalRecords,
    bytesRead = stats.bytesRead,
    bytesPerSecond = stats.bytesPerSecond,
    recordsDelta = stats.recordsDelta,
    recordsPerSecond = stats.recordsPerSecond,
    pctComplete = stats.pctComplete,
    completionTime = stats.completionTime.map(t => new Date(t.toLong))
  )

}
