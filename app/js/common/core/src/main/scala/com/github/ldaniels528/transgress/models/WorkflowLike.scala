package com.github.ldaniels528.transgress.models

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a workflow-like model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait WorkflowLike extends js.Object {

  def name: js.UndefOr[String]

  def input: js.UndefOr[SourceLike]

  def outputs: js.UndefOr[js.Array[SourceLike]]

  def events: js.UndefOr[js.Dictionary[OperationLike]]

  def variables: js.UndefOr[js.Array[VariableLike]]

}

/**
  * Represents a variable
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait VariableLike extends js.Object {

  def name: js.UndefOr[String]

}

/**
  * Represents a workflow source
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait SourceLike extends js.Object {

  def name: js.UndefOr[String]

  def path: js.UndefOr[String]

  def `type`: js.UndefOr[String]

  def format: js.UndefOr[String]

  def columnHeaders: js.UndefOr[Boolean]

  def fields: js.UndefOr[js.Array[FieldLike]]

  def mongoConnect: js.UndefOr[String]

  def mongoCollection: js.UndefOr[String]

}

/**
  * Represents a field
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait FieldLike extends js.Object {

  def name: js.UndefOr[String]

  def length: js.UndefOr[Int]

}