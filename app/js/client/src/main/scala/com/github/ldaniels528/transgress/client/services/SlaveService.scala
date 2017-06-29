package com.github.ldaniels528.transgress.client.services

import com.github.ldaniels528.transgress.client.models.Slave
import io.scalajs.npm.angularjs.Service
import io.scalajs.npm.angularjs.http.{Http, HttpResponse}

import scala.concurrent.ExecutionContext
import scala.scalajs.js

/**
  * Slave Service
  * @author lawrence.daniels@gmail.com
  */
class SlaveService($http: Http) extends Service {

  /**
    * Retrieves active slaves
    * @param ec the [[ExecutionContext]]
    * @return the promise of the resultant array of [[Slave]]s
    */
  def getSlaves()(implicit ec: ExecutionContext): js.Promise[HttpResponse[js.Array[Slave]]] = {
    $http.get("/api/slaves")
  }

}
