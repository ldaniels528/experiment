package com.github.ldaniels528.transgress.client
package services

import com.github.ldaniels528.transgress.RemoteEvent
import io.scalajs.JSON
import io.scalajs.dom.html.browser.{console, window}
import io.scalajs.dom.ws._
import io.scalajs.npm.angularjs._
import io.scalajs.npm.angularjs.http.Http
import io.scalajs.npm.angularjs.toaster.Toaster
import io.scalajs.util.DurationHelper._

import scala.concurrent.duration._

/**
  * Web Socket Service
  * @author lawrence.daniels@gmail.com
  */
class WebSocketService($rootScope: Scope, $http: Http, $location: Location, $timeout: Timeout, toaster: Toaster)
  extends Service {

  private var socket: WebSocket = _
  private var connected: Boolean = false
  private var attemptsLeft: Int = 3

  /**
    * Initializes the service
    */
  def init() {
    console.log("Initializing WebSocket service...")
    if (window.WebSocket.isEmpty) {
      console.log("Using a Mozilla Web Socket")
      window.WebSocket = window.MozWebSocket
    }

    window.WebSocket.toOption match {
      case Some(_) => connect()
      case None =>
        toaster.pop("Info", "Your browser does not support Web Sockets.")
    }
  }

  /**
    * Indicates whether a connection is established
    */
  def isConnected: Boolean = Option(socket).nonEmpty && connected

  /**
    * Transmits the message to the server via web-socket
    * @param message the given message
    */
  def send(message: String): Boolean = {
    window.WebSocket.toOption match {
      case Some(_) if socket.readyState == WebSocket.OPEN =>
        socket.send(message)
        true
      case Some(_) =>
        toaster.error(s"Web socket closed: readyState = ${socket.readyState}")
        false
      case None =>
        toaster.error("Web socket closed: window.WebSocket is not defined")
        false
    }
  }

  /**
    * Establishes a web socket connection
    */
  private def connect() {
    val endpoint = s"ws://${$location.host()}:${$location.port()}/websocket"
    console.log(s"Connecting to WebSocket endpoint '$endpoint'...")

    // open the connection and setup the handlers
    socket = new WebSocket(endpoint)

    socket.onopen = (event: OpenEvent) => {
      connected = true
      console.log("WebSocket connection established")
    }

    socket.onclose = (event: CloseEvent) => {
      connected = false
      console.warn("WebSocket connection lost")
      $timeout(() => connect(), 15.seconds)
    }

    socket.onerror = (event: ErrorEvent) => ()

    socket.onmessage = (event: MessageEvent) => handleMessage(event)

    $timeout(() => socket.send("Hello"), 5.seconds)
  }

  /**
    * Handles the incoming web socket message event
    * @param event the given web socket message event
    */
  private def handleMessage(event: MessageEvent) {
    Option(event.data) match {
      case Some(rawMessage: String) =>
        //console.log(s"rawMessage = '$rawMessage'")
        val message = JSON.parse(rawMessage).asInstanceOf[RemoteEvent]
        val result = for {
          action <- message.action.toOption
          data <- message.data.toOption.map(JSON.parse(_))
        } yield (action, data)

        result match {
          case Some((action, data)) => $rootScope.$broadcast(action, data)
          case None =>
            console.warn(s"Message does not contain either an 'action' or 'data' property: ${angular.toJson(message)}")
        }

      case None =>
        console.warn(s"Unhandled event received: ${angular.toJson(event)}")
    }
  }

}
