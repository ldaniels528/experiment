package com.github.ldaniels528.transgress.client.controllers

import io.scalajs.npm.angularjs.{Controller, Location, Scope}

import scala.scalajs.js
import scala.scalajs.js.annotation.ScalaJSDefined

/**
  * Tab Handling
  * @author lawrence.daniels@gmail.com
  */
trait TabHandling {
  self: Controller =>

  def $location: Location

  def $scope: Scope with TabHandlingScope

  /////////////////////////////////////////////////////////
  //    Initialization
  /////////////////////////////////////////////////////////

  $scope.tabs = js.Array(
    new AppTab(name = "Activity", uri = "/activity", icon = "fa-tasks"),
    new AppTab(name = "Dashboard", uri = "/dashboard", icon = "fa-stack-overflow"),
    //new AppTab(name = "Slaves", uri = "/slaves", icon = "fa-android"),
    //new AppTab(name = "Triggers", uri = "/triggers", icon = "fa-cogs"),
    new AppTab(name = "Workflows", uri = "/workflows", icon = "fa-th-large")
  )

  // the first tab is active
  $scope.selectedTab = $scope.tabs(0)

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  $scope.changeTab = (aTab: js.UndefOr[AppTab]) => aTab foreach { tab =>
    $scope.selectedTab = tab
    $location.url(tab.uri)
  }

}

/**
  * Tab Handling Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TabHandlingScope extends js.Any {
  self: Scope =>

  // variables
  var selectedTab: AppTab = js.native
  var tabs: js.Array[AppTab] = js.native

  // functions
  var changeTab: js.Function1[js.UndefOr[AppTab], Unit] = js.native

}

/**
  * Represents a tab
  * @param name the name of the tab
  * @param icon the icon of the tab
  */
@ScalaJSDefined
class AppTab(val name: String, val uri: String, val icon: String) extends js.Object