package com.github.ldaniels528.transgress.client
package models

import com.github.ldaniels528.transgress.models.JobStates._
import com.github.ldaniels528.transgress.models.{JobLike, StatisticsLike}

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a job model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
class Job(var _id: js.UndefOr[String] = js.undefined,
          var name: js.UndefOr[String] = js.undefined,
          var input: js.UndefOr[String] = js.undefined,
          var inputSize: js.UndefOr[Double] = js.undefined,
          var slaveID: js.UndefOr[String] = js.undefined,
          var workflowName: js.UndefOr[String] = js.undefined,
          var state: js.UndefOr[String] = js.undefined,
          var lastUpdated: js.UndefOr[Double] = js.undefined,
          var message: js.UndefOr[String] = js.undefined,
          var processingHost: js.UndefOr[String] = js.undefined,
          var statistics: js.UndefOr[StatisticsLike] = js.undefined)
  extends JobLike with Expandable

/**
  * Job Companion
  * @author lawrence.daniels@gmail.com
  */
object Job {

  implicit class JobExtensions(val job: Job) extends AnyVal {

    @inline
    def isUnfinished: Boolean = job.state.exists(!_.contains(SUCCESS))

  }

}