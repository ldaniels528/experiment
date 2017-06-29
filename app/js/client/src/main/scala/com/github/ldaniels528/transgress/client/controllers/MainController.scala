package com.github.ldaniels528.transgress.client
package controllers

import com.github.ldaniels528.transgress.AppConstants._
import com.github.ldaniels528.transgress.RemoteEvent.{JOB_UPDATE, SLAVE_UPDATE}
import com.github.ldaniels528.transgress.client.models.{Job, Slave}
import com.github.ldaniels528.transgress.client.services.{JobService, SlaveService}
import com.github.ldaniels528.transgress.models.JobStates
import com.github.ldaniels528.transgress.models.JobStates.JobState
import io.scalajs.dom.Event
import io.scalajs.dom.html.browser.console
import io.scalajs.npm.angularjs.AngularJsHelper._
import io.scalajs.npm.angularjs._
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.util.DurationHelper._
import io.scalajs.util.JsUnderOrHelper._

import scala.concurrent.duration._
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Main Controller
  * @author lawrence.daniels@gmail.com
  */
case class MainController($scope: MainScope, $interval: Interval, $location: Location, toaster: Toaster,
                          @injected("JobService") jobService: JobService,
                          @injected("SlaveService") slaveService: SlaveService)
  extends Controller with CollapseExpandHandling with JobHandling with SlaveHandling with TabHandling {

  /////////////////////////////////////////////////////////
  //    Initialization
  /////////////////////////////////////////////////////////

  $scope.version = String.valueOf(Version)

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  /**
    * Initializes the controller
    */
  $scope.init = () => {
    console.info(s"Initializing ${getClass.getSimpleName}...")

    // get the list of jobs and slaves
    val startTime = js.Date.now()
    val outcome = for {
      slaves <- slaveService.getSlaves().toFuture.map(_.data)
      jobs <- jobService.getJobs().toFuture.map(_.data)
    } yield (slaves, jobs)

    outcome onComplete {
      case Success((slaves, jobs)) =>
        console.log(s"Loaded slaves and jobs in ${js.Date.now() - startTime} msecs...")
        $scope.$apply { () =>
          $scope.slaves = slaves
          $scope.jobs = jobs

          slaves foreach { slave =>
            slave.jobs = jobs.filter(_.slaveID ?== slave._id)
          }
        }
      case Failure(e) =>
        console.log(s"Failed to retrieve slaves and jobs: ${e.getMessage}")
        e.printStackTrace()
    }

    // and update them every 3 minutes
    $interval(() => $scope.refreshJobs(js.undefined), 3.minute)
  }

  /**
    * Retrieves the corresponding slave for the given job
    */
  $scope.getSlaveForJob = (aJob: js.UndefOr[Job]) => aJob flatMap { job =>
    $scope.slaves.find(_._id ?== job.slaveID).orUndefined
  }

  /**
    * Pauses the given job
    */
  $scope.pauseJob = (aJob: js.UndefOr[Job]) => {
    for {
      job <- aJob
      jobId <- job._id
      slave <- $scope.getSlaveForJob(job)
      slaveId <- slave._id
    } {
      $scope.pausing = true
      jobService.pauseJob(jobId, slaveId).toFuture onComplete {
        case Success(response) =>
          $scope.$apply { () =>
            $scope.pausing = false
            $scope.updateJob($scope.jobs, response.data)
          }
        case Failure(e) =>
          $scope.pausing = false
          toaster.error("Failed to pause job")
          console.error(s"Failed to pause job: ${e.displayMessage}")
      }
    }
  }

  $scope.refreshJobs = (aJobStates: js.UndefOr[js.Array[JobState]]) => {
    val jobStates = aJobStates getOrElse JobStates.values.toJSArray
    jobService.getJobs(jobStates: _*).toFuture onComplete {
      case Success(response) =>
        $scope.$apply { () =>
          response.data foreach { job =>
            $scope.updateJob($scope.jobs, job)
            updateSlaveJobs(job)
          }
        }
      case Failure(e) =>
        console.error(e.displayMessage)
    }
  }

  $scope.refreshSlaves = () => {
    slaveService.getSlaves().toFuture onComplete {
      case Success(response) =>
        $scope.$apply { () =>
          $scope.slaves = response.data
        }
      case Failure(e) =>
        toaster.error("Error loading slave")
        console.error(s"Error loading slave: ${e.displayMessage}")
    }
  }

  /**
    * Resumes the given job
    */
  $scope.resumeJob = (aJob: js.UndefOr[Job]) => {
    for {
      job <- aJob
      jobId <- job._id
      slave <- $scope.getSlaveForJob(job)
      slaveId <- slave._id
    } {
      $scope.resuming = true
      jobService.resumeJob(jobId, slaveId).toFuture onComplete {
        case Success(response) =>
          $scope.$apply { () =>
            $scope.resuming = false
            $scope.updateJob($scope.jobs, response.data)
          }
        case Failure(e) =>
          $scope.resuming = false
          toaster.error("Failed to resume job")
          console.error(s"Failed to resume job: ${e.displayMessage}")
      }
    }
  }

  /**
    * Stops the given job
    */
  $scope.stopJob = (aJob: js.UndefOr[Job]) => {
    for {
      job <- aJob
      jobId <- job._id
      slave <- $scope.getSlaveForJob(job)
      slaveId <- slave._id
    } {
      $scope.stopping = true
      jobService.stopJob(jobId, slaveId).toFuture onComplete {
        case Success(response) =>
          $scope.$apply { () =>
            $scope.stopping = false
            $scope.updateJob($scope.jobs, response.data)
          }
        case Failure(e) =>
          $scope.stopping = false
          toaster.error("Failed to stop job")
          console.error(s"Failed to stop job: ${e.displayMessage}")
      }
    }
  }

  /////////////////////////////////////////////////////////
  //    Private Methods
  /////////////////////////////////////////////////////////

  def updateSlaveJobs(job: Job) {
    $scope.getSlaveForJob(job) foreach { slave =>
      if (slave.jobs.isEmpty) slave.jobs = new js.Array[Job]()
      $scope.updateJob(slave.jobs, job)
    }
  }

  /////////////////////////////////////////////////////////
  //    Event Listeners
  /////////////////////////////////////////////////////////

  $scope.$on(JOB_UPDATE, (_: Event, job: Job) => {
    $scope.$apply { () =>
      $scope.updateJob($scope.jobs, job)
      updateSlaveJobs(job)
    }
  })

  $scope.$on(SLAVE_UPDATE, (_: Event, slave: Slave) => {
    $scope.$apply(() => $scope.updateSlave($scope.slaves, slave))
  })


}

/**
  * Main Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait MainScope extends Scope
  with CollapseExpandHandlingScope with JobHandlingScope with SlaveHandlingScope with TabHandlingScope {

  // variables
  var version: String = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var getSlaveForJob: js.Function1[js.UndefOr[Job], js.UndefOr[Slave]] = js.native
  var pauseJob: js.Function1[js.UndefOr[Job], Unit] = js.native
  var refreshJobs: js.Function1[js.UndefOr[js.Array[JobState]], Unit] = js.native
  var refreshSlaves: js.Function0[Unit] = js.native
  var resumeJob: js.Function1[js.UndefOr[Job], Unit] = js.native
  var stopJob: js.Function1[js.UndefOr[Job], Unit] = js.native

}

