package com.qwery.database.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.QweryCustomJsonProtocol._
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.ExecutionContext
import scala.language.postfixOps
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
   * Define the API routes
   * @param service the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  def route()(implicit service: ServerSideTableService): Route = {
    // routes: /<database> (e.g. "/portfolio")
    path(Segment) {
      routesByDatabase
    } ~
      // routes: /<database>/<table> (e.g. "/portfolio/stocks")
      path(Segment / Segment) {
        routesByDatabaseTable
      } ~
      // routes: /<database>/<table>/<rowID> (e.g. "/portfolio/stocks/187")
      path(Segment / Segment / IntNumber) {
        routesByDatabaseTableRowID
      } ~
      // routes: /<database>/<table>/length (e.g. "/portfolio/stocks/length")
      path(Segment / Segment / "length") {
        routesByDatabaseTableLength
      } ~
      // routes: /<database>/<table>/<rowID>/<count> (e.g. "/portfolio/stocks/187/23")
      path(Segment / Segment / IntNumber / IntNumber) {
        routesByDatabaseTableRange
      }
  }

  /**
   * Database-specific routes
   * @param databaseName the name of the database
   * @param service the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabase(databaseName: String)(implicit service: ServerSideTableService): Route = {
    get {
      // retrieve the database metrics (e.g. "GET /portfolio")
      complete(service.getDatabaseMetrics(databaseName))
    } ~
      post {
        // executes a SQL query (e.g. "POST /portfolio" <~ { sql: "TRUNCATE TABLE staging" })
        entity(as[String]) { sql =>
          complete(service.executeQuery(databaseName, sql).map(_.toMap))
        }
      }
  }

  /**
   * Database Table-specific routes
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTable(databaseName: String, tableName: String)(implicit service: ServerSideTableService): Route = {
    delete {
      // drops a table by name
      // (e.g. "DELETE /portfolio/stocks")
      complete(service.dropTable(databaseName, tableName).toLiftJs.toSprayJs)
    } ~
      get {
        // retrieve the table metrics (e.g. "GET /portfolio/stocks")
        // or query via query parameters (e.g. "GET /portfolio/stocks?exchange=AMEX&__limit=5")
        extract(_.request.uri.query()) { params =>
          val (limit, condition) = (params.get("__limit").map(_.toInt), toValues(params))
            complete(
              if (params.isEmpty) service.getTableMetrics(databaseName, tableName)
              else service.findRows(databaseName, tableName, condition, limit).map(_.toMap)
            )
          }
      } ~
      post {
        // appends a new record into a table by name
        // (e.g. "POST /portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          val values = jsObject.fields.map { case (k, js) => (k, js.unwrapJSON) }
          complete(service.appendRow(databaseName, tableName, values).toLiftJs.toSprayJs)
        }
      }
  }

  /**
   * Database Table Length-specific routes
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableLength(databaseName: String, tableName: String)(implicit service: ServerSideTableService): Route = {
    get {
      // retrieve the length of the table (e.g. "GET /portfolio/stocks/287/length")
      complete(service.getLength(databaseName, tableName))
    }
  }

  /**
   * Database Table Range-specific routes
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param start        the start of the range
   * @param length       the number of rows referenced
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRange(databaseName: String, tableName: String, start: Int, length: Int)(implicit service: ServerSideTableService): Route = {
    get {
      // retrieve a range of rows (e.g. "GET /portfolio/stocks/287/20")
      complete(service.getRange(databaseName, tableName, start, length).map(_.toMap))
    }
  }

  /**
   * Database Table Length-specific routes
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the referenced row ID
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRowID(databaseName: String, tableName: String, rowID: Int)(implicit service: ServerSideTableService): Route = {
    delete {
      // delete a row by index
      // (e.g. "DELETE /portfolio/stocks/129")
      complete(service.deleteRow(databaseName, tableName, rowID).toLiftJs.toSprayJs)
    } ~
      get {
        // retrieve a row by index
        // (e.g. "GET /portfolio/stocks/287")
        service.getRow(databaseName, tableName, rowID) match {
          case Some(row) => complete(row.toMap.toJson)
          case None => complete(JsObject())
        }
      } ~
      post {
        // partially updates a row by index into a table by name
        // (e.g. "POST /portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          val values = jsObject.fields.map { case (k, js) => (k, js.unwrapJSON) }
          complete(service.replaceRow(databaseName, tableName, rowID, values))
        }
      } ~
      put {
        // replaces a row by index into a table by name
        // (e.g. "PUT /portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          val values = jsObject.fields.map { case (k, js) => (k, js.unwrapJSON) }
          complete(service.replaceRow(databaseName, tableName, rowID, values))
        }
      }
  }

  private def toValues(params: Uri.Query): TupleSet = Map(params.filterNot(_._1.name.startsWith("__")): _*)

}
