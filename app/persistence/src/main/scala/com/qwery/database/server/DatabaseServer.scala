package com.qwery.database.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import com.qwery.database.server.QweryCustomJsonProtocol._
import org.slf4j.LoggerFactory
import spray.json._

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

    // display the application version
    val version = 0.1
    logger.info(f"Qwery Database Server v$version%.1f")

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
    implicit val tableServices: TableServices = TableServices()

    // bind to the port
    val bindingFuture = Http().bindAndHandle(route(), host, port)
    bindingFuture.onComplete {
      case Success(serverBinding) =>
        logger.info(s"listening to ${serverBinding.localAddress}")
      case Failure(e) =>
        logger.error(s"error: ${e.getMessage}", e)
    }
  }

  /**
   * Define the routes
   * @param tables   the implicit [[TableManager]]
   * @param services the implicit [[TableServices]]
   * @return the [[Route]]
   */
  private def route()(implicit tables: TableManager, services: TableServices): Route = {
    pathPrefix("tables") {
      path(Segment) { tableName =>
        get {
          // retrieve the table statistics (e.g. "GET /tables/stocks")
          // or query via query parameters (e.g. "GET /tables/stocks?exchange=AMEX&__limit=5")
          extract(_.request.uri.query()) { params =>
            val limit = params.get("__limit").map(_.toInt)
            val values = toValues(params).filterNot(_._1.name.startsWith("__"))
            complete(if (params.isEmpty) services.getStatistics(tableName).toJson else services.find(tableName, values, limit).toJson)
          }
        } ~
          delete {
            // delete rows by criteria (e.g. "DEL /tables/stocks?symbol=AAPL")
            extract(_.request.uri.query()) { params =>
              complete(services.deleteRows(tableName, condition = toValues(params)).toJson)
            }
          }
      } ~
        path(Segment / IntNumber) { (tableName, rowID) =>
          get {
            // retrieve a row by index (e.g. "GET /tables/stocks/287")
            complete(services.getRow(tableName, rowID).toJson)
          } ~
            delete {
              // delete a row by index (e.g. "DEL /tables/stocks/129")
              complete(services.deleteRow(tableName, rowID).toJson)
            }
        } ~
        path(Segment / IntNumber / IntNumber) { (tableName, start, length) =>
          get {
            // retrieve a range of rows (e.g. "GET /tables/stocks/287/20")
            complete(services.getRows(tableName, start, length).toJson)
          }
        }
    }
  }

  private def toValues(params: Uri.Query): Map[Symbol, String] = {
    Map(params.map { case (k, v) => (Symbol(k), v) }: _*)
  }

}
