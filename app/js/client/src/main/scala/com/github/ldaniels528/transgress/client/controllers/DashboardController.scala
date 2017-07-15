package com.github.ldaniels528.transgress.client
package controllers

import io.scalajs.util.OptionHelper._
import com.github.ldaniels528.transgress.client.models.Job
import com.github.ldaniels528.transgress.models.JobStates
import io.scalajs.dom.html.browser._
import io.scalajs.npm.angularjs.{Controller, Scope}

import scala.scalajs.js

/**
  * Dashboard Controller
  * @author lawrence.daniels@gmail.com
  */
class DashboardController($scope: DashboardScope) extends Controller {

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  /**
    * Initializes the controller
    */
  $scope.init = () => {
    console.info(s"Initializing ${getClass.getSimpleName}...")
  }

  $scope.getDashboardJobs = () => {
    $scope.jobs.filterNot(_.state.contains(JobStates.SUCCESS))
  }


}

/**
  * Dashboard Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait DashboardScope extends Scope with JobHandlingScope {
  // functions
  var init: js.Function0[Unit] = js.native
  var getDashboardJobs: js.Function0[js.Array[Job]] = js.native

}
