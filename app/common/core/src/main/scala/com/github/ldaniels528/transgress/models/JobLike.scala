package com.github.ldaniels528.transgress.models

import com.github.ldaniels528.transgress.models.JobStates.JobState

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a job-like model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait JobLike extends js.Object {

  def name: js.UndefOr[String]

  def input: js.UndefOr[String]

  def inputSize: js.UndefOr[Double]

  def lastUpdated: js.UndefOr[Double]

  def message: js.UndefOr[String]

  def processingHost: js.UndefOr[String]

  def slaveID: js.UndefOr[String]

  def state: js.UndefOr[JobState]

  def statistics: js.UndefOr[StatisticsLike]

  def workflowName: js.UndefOr[String]

}

