package com.github.ldaniels528.transgress.client.controllers

import com.github.ldaniels528.transgress.client.models.Slave
import com.github.ldaniels528.transgress.client.services.SlaveService
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.npm.angularjs.{Controller, Scope}

import scala.scalajs.js

/**
  * Slave Handling
  * @author lawrence.daniels@gmail.com
  */
trait SlaveHandling {
  self: Controller =>

  def $scope: Scope with SlaveHandlingScope

  def slaveService: SlaveService

  def toaster: Toaster

  /////////////////////////////////////////////////////////
  //    Initialization
  /////////////////////////////////////////////////////////

  $scope.slaves = js.Array()

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  $scope.updateSlave = (aSlaves: js.UndefOr[js.Array[Slave]], aSlave: js.UndefOr[Slave]) => {
    for {
      slaves <- aSlaves
      slave <- aSlave
    } {
      slaves.indexWhere(_._id == slave._id) match {
        case -1 => slaves.push(slave)
        case index =>
          val theSlave = slaves(index)
          theSlave.concurrency = slave.concurrency
          theSlave.maxConcurrency = slave.maxConcurrency
          theSlave.lastUpdated = slave.lastUpdated
          theSlave.jobs = theSlave.jobs.map(_.filter(_.isUnfinished))
      }
    }
  }

  /**
    * Computes the slave's current utilization
    */
  $scope.utilization = (aSlave: js.UndefOr[Slave]) => {
    for {
      slave <- aSlave
      concurrency <- slave.concurrency
      maxConcurrency <- slave.maxConcurrency if maxConcurrency > 0
    } yield 100.0 * (concurrency / maxConcurrency.toDouble)
  }

}

/**
  * Slave Handling Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SlaveHandlingScope extends js.Any {
  self: Scope =>

  // variables
  var slaves: js.Array[Slave] = js.native

  // functions
  var updateSlave: js.Function2[js.UndefOr[js.Array[Slave]], js.UndefOr[Slave], Unit] = js.native
  var utilization: js.Function1[js.UndefOr[Slave], js.UndefOr[Double]] = js.native

}