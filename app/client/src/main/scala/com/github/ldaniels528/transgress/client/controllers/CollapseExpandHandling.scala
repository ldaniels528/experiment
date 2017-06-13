package com.github.ldaniels528.transgress.client.controllers

import com.github.ldaniels528.transgress.client.models.Expandable
import io.scalajs.npm.angularjs.{Controller, Scope}
import io.scalajs.util.JsUnderOrHelper._

import scala.scalajs.js

/**
  * Collapse-Expand Handling
  * @author lawrence.daniels@gmail.com
  */
trait CollapseExpandHandling {
  self: Controller =>

  def $scope: Scope with CollapseExpandHandlingScope

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  $scope.collapseExpand = (anExpandable: js.UndefOr[Expandable]) => anExpandable foreach { expandable =>
    expandable.expanded = !expandable.expanded.isTrue
  }

}

/**
  * Collapse-Expand Handling Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait CollapseExpandHandlingScope extends js.Any {
  self: Scope =>

  // functions
  var collapseExpand: js.Function1[js.UndefOr[Expandable], Unit] = js.native

}