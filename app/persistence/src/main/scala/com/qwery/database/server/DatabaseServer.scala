package com.qwery.database.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Database Server
 */
object DatabaseServer {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Main program
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val defaultPort = 8233
    val defaultPoolSize = 25

    // get the bind/listen port and pool size
    val (port, poolSize) = args match {
      case Array(port, poolSize, _*) => (port.toInt, poolSize.toInt)
      case Array(port, _*) => (port.toInt, defaultPoolSize)
      case Array() => (defaultPort, defaultPoolSize)
    }

    // create the actor pool
    implicit val system: ActorSystem = ActorSystem(name = "database-server")
    import system.dispatcher

    // start the server
    startServer(port = port)
  }

  /**
   * Starts the server
   * @param host   the server host (e.g. "0.0.0.0")
   * @param port   the server port
   * @param ec     the [[ExecutionContext]]
   * @param system the [[ActorSystem]]
   */
  def startServer(host: String = "0.0.0.0", port: Int)(implicit ec: ExecutionContext, system: ActorSystem): Unit = {
    implicit val tableManager: TableManager = new TableManager()

    // bind to the port
    val bindingFuture = Http().bindAndHandle(route(), host, port)
    bindingFuture.onComplete {
      case Success(serverBinding) =>
        logger.info(s"listening to ${serverBinding.localAddress}")
      case Failure(e) =>
        logger.error(s"error: ${e.getMessage}", e)
    }
  }

  private def route()(implicit tableManager: TableManager): Route = {
    path("table" / Segment / IntNumber) { (tableName, rowID) =>
      val table = tableManager.get(tableName)
      val row = table.get(rowID)
      complete(row.toString)
    } ~
      path("table" / Segment / IntNumber / "meta") { (tableName, rowID) =>
        val table = tableManager.get(tableName)
        val metadata = table.device.readRowMetaData(rowID)
        complete(metadata.toString)
      }
  }

}
