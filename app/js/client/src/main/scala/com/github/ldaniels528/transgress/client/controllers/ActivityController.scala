package com.github.ldaniels528.transgress.client
package controllers

import io.scalajs.dom.html.browser.console
import io.scalajs.npm.angularjs.{Controller, Scope}

import scala.scalajs.js

/**
  * Activity Controller
  * @author lawrence.daniels@gmail.com
  */
class ActivityController($scope: ActivityScope) extends Controller {

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
  * Activity Scope
  * @author lawrence.daniels@gmail.com
  */
@js.native
trait ActivityScope extends Scope with SlaveHandlingScope {

  // functions
  var init: js.Function0[Unit] = js.native

}
