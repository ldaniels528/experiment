package com.github.ldaniels528.transgress.client
package services

import com.github.ldaniels528.transgress.client.models.Workflow
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.scalajs.js

/**
  * Workflow Service
  * @author lawrence.daniels@gmail.com
  */
class WorkflowService($http: Http) extends Service {

  def getWorkflows: js.Promise[HttpResponse[js.Array[Workflow]]] = $http.get("/api/workflows")

}
