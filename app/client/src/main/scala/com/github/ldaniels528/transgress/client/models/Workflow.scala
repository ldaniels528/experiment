package com.github.ldaniels528.transgress.client
package models

import com.github.ldaniels528.transgress.models._

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Represents a workflow model
  * @author lawrence.daniels@gmail.com
  */
@ScalaJSDefined
trait Workflow extends WorkflowLike with Expandable {
  var _id: js.UndefOr[String] = js.undefined
  var name: js.UndefOr[String] = js.undefined
  var input: js.UndefOr[SourceLike] = js.undefined
  var outputs: js.UndefOr[js.Array[SourceLike]] = js.undefined
  var events: js.UndefOr[js.Dictionary[OperationLike]] = js.undefined
  var variables: js.UndefOr[js.Array[VariableLike]] = js.undefined

}
