package com.github.ldaniels528.transgress.client
package controllers

import io.scalajs.dom.html.browser.console
import io.scalajs.npm.angularjs.{Controller, Scope}

import scala.scalajs.js

/**
  * Slave Controller
  * @author lawrence.daniels@gmail.com
  */
class SlaveController($scope: SlaveScope) extends Controller {

  /////////////////////////////////////////////////////////
  //    Public Methods
  /////////////////////////////////////////////////////////

  /**
    * Initializes the controller
    */
  $scope.init = () => {
    console.info(s"Initializing ${getClass.getSimpleName}...")
  }

}

/**
  * Slave Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait SlaveScope extends Scope with SlaveHandlingScope {

  // functions
  var init: js.Function0[Unit] = js.native

}