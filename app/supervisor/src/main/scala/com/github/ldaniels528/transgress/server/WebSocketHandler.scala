package com.github.ldaniels528.transgress.server

import java.util.UUID

import com.github.ldaniels528.transgress.RemoteEvent
import com.github.ldaniels528.transgress.LoggerFactory
import io.scalajs.nodejs._
import io.scalajs.nodejs.timers.Immediate
import io.scalajs.npm.express.Request
import io.scalajs.npm.expressws.WebSocket

import scala.scalajs.js
import scala.scalajs.js.JSON
import scala.util.{Failure, Success, Try}

/**
  * WebSocket Handler
  * @author lawrence.daniels@gmail.com
  */
object WebSocketHandler {
  private val logger = LoggerFactory.getLogger(getClass)
  private val clients = js.Array[WsClient]()

  def messageHandler(ws: WebSocket, request: Request, message: String): Unit = {
    // handle the message
    message match {
      case "Hello" =>
        // have we received a message from this client before?
        val client = WsClient(ip = request.ip, ws = ws)
        logger.log(s"Client ${client.uid} (${client.ip}) connected")
        clients.push(client)
      case unknown =>
        logger.warn(s"Unhandled message '$unknown'...")
    }
  }

  def emit(action: String, data: js.Any): Immediate = emit(action, JSON.stringify(data))

  def emit(action: String, data: String): Immediate = {
    setImmediate(() => {
      //logger.log(s"Broadcasting action '$action' with data '$data'...")
      clients.foreach(client => Try(client.send(action, data)) match {
        case Success(_) =>
        case Failure(e) =>
          logger.warn(s"Client connection ${client.uid} (${client.ip}) failed")
          clients.indexWhere(_.uid == client.uid) match {
            case -1 => logger.error(s"Client ${client.uid} was not removed")
            case index => clients.remove(index)
          }
      })
    })
  }

  /**
    * Represents a web-socket client
    * @param ws the given [[WebSocket web socket]]
    */
  case class WsClient(ip: String, ws: WebSocket) {
    val uid: String = UUID.randomUUID().toString

    def send(action: String, data: String): js.Any = ws.send(encode(action, data))

    private def encode(action: String, data: String) = JSON.stringify(new RemoteEvent(action, data))

  }

}
