package com.github.ldaniels528.transgress.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a Trigger-like model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait TriggerLike extends js.Object {

  def name: js.UndefOr[String]

  def priority: js.UndefOr[Int]

  def patterns: js.UndefOr[js.Array[String]]

  def workflowName: js.UndefOr[String]

}
