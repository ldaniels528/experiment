package com.github.ldaniels528.qwery.etl.actors

import java.util.Date

import play.api.libs.json.Json

/**
  * Represents a Job
  * @author lawrence.daniels@gmail.com
  */
case class Job(_id: Option[String] = None,
               name: Option[String],
               input: Option[String],
               inputSize: Option[Double],
               state: Option[String],
               workflowName: Option[String],
               lastUpdated: Option[Date] = None,
               processingHost: Option[String],
               slaveID: Option[String],
               message: Option[String] = None,
               statistics: Option[Seq[JobStatistics]] = None)

/**
  * Job Companion
  * @author lawrence.daniels@gmail.com
  */
object Job {

  implicit val jobFormat = Json.format[Job]

}
