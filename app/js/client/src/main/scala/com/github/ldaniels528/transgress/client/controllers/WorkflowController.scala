package com.github.ldaniels528.transgress.client
package controllers

import com.github.ldaniels528.transgress.client.models.Workflow
import com.github.ldaniels528.transgress.client.services.WorkflowService
import io.scalajs.JSON
import io.scalajs.dom.html.browser.console
import io.scalajs.npm.angularjs.AngularJsHelper._
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.npm.angularjs.{Controller, Scope, injected}

import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js
import scala.scalajs.js.JSConverters._
import scala.util.{Failure, Success}

/**
  * Workflow Controller
  * @author lawrence.daniels@gmail.com
  */
class WorkflowController($scope: WorkflowScope, toaster: Toaster,
                         @injected("WorkflowService") workflowService: WorkflowService)
  extends Controller {

  /////////////////////////////////////////////////////////
  //    Variables
  /////////////////////////////////////////////////////////

  $scope.workflows = js.Array()

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  /**
    * Initializes the controller
    */
  $scope.init = () => {
    console.info(s"Initializing ${getClass.getSimpleName}...")
    refreshWorkflows()
  }

  /**
    * Selects a worfkflow
    */
  $scope.selectWorkflow = (aWorkflow: js.UndefOr[Workflow]) => $scope.selectedWorkflow = aWorkflow

  /**
    * Converts an object to JSON
    */
  $scope.toJSON = (anObject: js.Any) => JSON.stringify(anObject, null, 4)

  /////////////////////////////////////////////////////////
  //    Private Methods
  /////////////////////////////////////////////////////////

  private def refreshWorkflows(): Unit = {
    workflowService.getWorkflows.toFuture onComplete {
      case Success(response) =>
        console.log(s"Loaded ${response.data.length} workflow(s)")
        $scope.$apply(() => {
          response.data.foreach(workflow => updateWorkflow($scope.workflows, workflow))
          $scope.selectedWorkflow = response.data.headOption.orUndefined
        })
      case Failure(e) =>
        toaster.error("Initialization Error", e.displayMessage)
        console.error(e.displayMessage)
    }
  }

  private def updateWorkflow(workflows: js.Array[Workflow], workflow: Workflow): Unit = {
    workflows.indexWhere(_._id == workflow._id) match {
      case -1 => workflows.push(workflow)
      case index =>
        val theWorkflow = workflows(index)
        theWorkflow.name = workflow.name
        theWorkflow.input = workflow.input
        theWorkflow.outputs = workflow.outputs
        theWorkflow.events = workflow.events
        theWorkflow.variables = workflow.variables
    }
  }

}


/**
  * Workflow Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait WorkflowScope extends Scope {
  // variables
  var selectedWorkflow: js.UndefOr[Workflow] = js.native
  var workflows: js.Array[Workflow] = js.native

  // functions
  var init: js.Function0[Unit] = js.native
  var selectWorkflow: js.Function1[js.UndefOr[Workflow], Unit] = js.native
  var toJSON: js.Function1[js.Any, String] = js.native

}