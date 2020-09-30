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

    // display the application version
    val version = 0.1
      logger.info(f"QWERY Database Server v$version%.1f")

    // get the bind/listen port
    val port = args match {
      case Array(port, _*) => port.toInt
      case Array() => defaultPort
    }

    // create the actor pool
    implicit val system: ActorSystem = ActorSystem(name = "database-server")
    implicit val service: ServerSideTableService = new ServerSideTableService()
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
  def startServer(host: String = "0.0.0.0", port: Int)
                 (implicit ec: ExecutionContext, service: ServerSideTableService, system: ActorSystem): Unit = {
    // bind to the port
    val bindingFuture = Http().bindAndHandle(route(), host, port)
    bindingFuture.onComplete {
      case Success(serverBinding) =>
        logger.info(s"listening to ${serverBinding.localAddress}")
      case Failure(e) =>
        logger.error(s"Error: ${e.getMessage}", e)
    }
  }

  /**
   * Define the route
   * @param service the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def route()(implicit service: ServerSideTableService): Route = {
    // routes: /portfolio
    path(Segment) { databaseName =>
      // retrieve the table metrics (e.g. "GET /portfolio")
      complete(service.getDatabaseMetrics(databaseName).toJson)
    } ~
    // routes: /portfolio/stocks
    path(Segment / Segment) { (databaseName, tableName) =>
      get {
        extract(_.request.uri.query()) { params =>
          val (limit, condition) = (params.get("__limit").map(_.toInt), toValues(params))
          complete(
            // retrieve the table metrics (e.g. "GET /portfolio/stocks")
            if (params.isEmpty) service.getTableMetrics(databaseName, tableName).toJson
            // or query via query parameters (e.g. "GET /portfolio/stocks?exchange=AMEX&__limit=5")
            else service.findRows(databaseName, tableName, condition, limit).toJson
          )
        }
      } ~
          post {
            // appends a new record into a table by name
            // (e.g. "POST /portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
            entity(as[JsObject]) { jsObject =>
              val record = jsObject.fields.map { case (k, js) => (k, unwrap(js)) }
              complete(service.appendRow(databaseName, tableName, record).toJson)
            }
          } ~
        delete {
          // drops a table by name (e.g. "DEL /portfolio/stocks")
          complete(service.dropTable(databaseName, tableName).toJson)
        }
    } ~
      // routes: /portfolio/stocks/187
      path(Segment / Segment / IntNumber) { (databaseName, tableName, rowID) =>
        get {
          // retrieve a row by index (e.g. "GET /portfolio/stocks/287")
          service.getRow(databaseName, tableName, rowID) match {
            case Some(row) => complete(row.toJson)
            case None => complete(JsObject())
          }
        } ~
          delete {
            // delete a row by index (e.g. "DEL /portfolio/stocks/129")
            complete(service.deleteRow(databaseName, tableName, rowID).toJson)
          }
      } ~
      // routes: /portfolio/stocks/187/65
      path(Segment / Segment / IntNumber / IntNumber) { (databaseName, tableName, start, length) =>
        get {
          // retrieve a range of rows (e.g. "GET /portfolio/stocks/287/20")
          complete(service.getRange(databaseName, tableName, start, length).toJson)
        }
      } ~
      path(Segment / "sql") { databaseName =>
        post {
          // routes: /portfolio/sql <~ { sql: "SELECT ..." }
          entity(as[String]) { sql =>
            complete(service.executeQuery(databaseName, sql))
          }
        }
      }
  }

  private def toValues(params: Uri.Query): TupleSet = {
    Map(params.filterNot(_._1.name.startsWith("__")): _*)
  }

}
