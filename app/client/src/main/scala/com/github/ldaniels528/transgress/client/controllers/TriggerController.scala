package com.github.ldaniels528.transgress.client
package controllers

import io.scalajs.dom.html.browser.console
import io.scalajs.npm.angularjs.{Controller, Scope}

import scala.scalajs.js

/**
  * Trigger Controller
  * @author lawrence.daniels@gmail.com
  */
class TriggerController($scope: TriggerScope) extends Controller {

  /////////////////////////////////////////////////////////
  //    Variables
  /////////////////////////////////////////////////////////


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
  * Trigger Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait TriggerScope extends Scope {
  // variables

  // functions
  var init: js.Function0[Unit] = js.native

}