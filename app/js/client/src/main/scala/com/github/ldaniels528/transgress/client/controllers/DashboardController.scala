package com.github.ldaniels528.transgress.client
package controllers

import com.github.ldaniels528.transgress.client.models.Job
import com.github.ldaniels528.transgress.models.JobStates
import io.scalajs.dom.html.browser._
import io.scalajs.npm.angularjs.{Controller, Scope, angular}

import scala.scalajs.js
import scala.scalajs.js.JSConverters._

/**
  * Dashboard Controller
  * @author lawrence.daniels@gmail.com
  */
class DashboardController($scope: DashboardScope) extends Controller {

  $scope.jobStates = JobStates.values.map(_.toString).toJSArray
  $scope.selectedJobState = $scope.jobStates.headOption.orUndefined

  $scope.maxResultsSet = js.Array("25", "50", "100", "250")
  $scope.maxResults = $scope.maxResultsSet.headOption.orUndefined

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  /**
    * Initializes the controller
    */
  $scope.init = () => {
    console.info(s"Initializing ${getClass.getSimpleName}...")
  }

  $scope.getJobCompletion = (aJob: js.UndefOr[Job]) => {
    for {
      job <- aJob
      statistics <- job.statistics
      stats <- statistics.find(_.pctComplete.isDefined).orUndefined
      pctComplete <- stats.pctComplete
    } yield pctComplete
  }

  $scope.getDashboardJobs = () => {
    var jobs = $scope.jobs.filter(_.state == $scope.selectedJobState)
    $scope.maxResults.map(_.toInt).map(count => jobs.take(count)) getOrElse jobs
  }

  $scope.updateJobState = (aState: js.UndefOr[String]) => {
    $scope.selectedJobState = aState
  }

}

/**
  * Dashboard Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait DashboardScope extends Scope with JobHandlingScope {
  // variables
  var jobStates: js.Array[String] = js.native
  var maxResults: js.UndefOr[String] = js.native
  var maxResultsSet: js.Array[String] = js.native
  var selectedJobState: js.UndefOr[String] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var getDashboardJobs: js.Function0[js.Array[Job]] = js.native
  var getJobCompletion: js.Function1[js.UndefOr[Job], js.UndefOr[Double]] = js.native
  var updateJobState: js.Function1[js.UndefOr[String], Unit] = js.native

}
