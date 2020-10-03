package com.qwery.database.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import com.qwery.database.server.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.database.server.QweryCustomJsonProtocol._
import com.qwery.database.server.TableService._
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
    // route: /<database> (e.g. "/portfolio")
    path(Segment)(routesByDatabase) ~
      // route: /<database>/<table> (e.g. "/portfolio/stocks")
      path(Segment / Segment)(routesByDatabaseTable) ~
      // route: /<database>/<table>/<rowID> (e.g. "/portfolio/stocks/187")
      path(Segment / Segment / IntNumber)(routesByDatabaseTableRowID) ~
      // route: /<database>/<table>/<rowID>/<count> (e.g. "/portfolio/stocks/187/23")
      path(Segment / Segment / IntNumber / IntNumber)(routesByDatabaseTableRange) ~
      // route: /<database>/<table>/export/<format>/<fileName> (e.g. "/portfolio/stocks/export/json/stocks.json")
      path(Segment / Segment / "export" / Segment / Segment)(routesByDatabaseTableExport) ~
      // route: /<database>/<table>/length (e.g. "/portfolio/stocks/length")
      path(Segment / Segment / "length")(routesByDatabaseTableLength) ~
      // route: /<database>/<table>/sql (e.g. "/portfolio/stocks/sql")
      path(Segment / Segment / "sql")(routesByDatabaseTableSQL)
  }

  /**
   * Database-specific API routes (e.g. "/portfolio")
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
        // create the new table (e.g. "POST /portfolio/stocks" <~ {"tableName":"stocks_client_test_0", "columns":[...]}
        entity(as[String]) { jsonString =>
          complete(service.createTable(databaseName, jsonString.fromJSON[TableCreation]))
        }
      }
  }

  /**
   * Database Table-specific API routes (e.g. "/portfolio/stocks")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTable(databaseName: String, tableName: String)(implicit service: ServerSideTableService): Route = {
    delete {
      // drop the table by name (e.g. "DELETE /portfolio/stocks")
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
        // append the new record to the table
        // (e.g. "POST /portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(service.appendRow(databaseName, tableName, toValues(jsObject)))
        }
      }
  }

  /**
   * Database Table Export-specific API routes (e.g. "/portfolio/stocks/export/csv/stocks.csv")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param format       the destination file format (e.g. "csv", "json", "bin")
   * @param fileName     the name of the file to be downloaded
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableExport(databaseName: String, tableName: String, format: String, fileName: String)
                                         (implicit service: ServerSideTableService): Route = {
    val dataFile = format.toLowerCase() match {
      case "csv" => service(databaseName, tableName).exportAsCSV
      case "json" => service(databaseName, tableName).exportAsJSON
      case "bin" => TableFile.getTableDataFile(databaseName, tableName)
      case other => throw new IllegalArgumentException(s"Unsupported file format '$other'")
    }
    logger.info(s"Exporting '$fileName' (as ${format.toUpperCase()}) <~ ${dataFile.getAbsolutePath}")
    getFromFile(dataFile)
  }

  /**
   * Database Table Length-specific API routes (e.g. "/portfolio/stocks/length")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableLength(databaseName: String, tableName: String)(implicit service: ServerSideTableService): Route = {
    get {
      // retrieve the length of the table (e.g. "GET /portfolio/stocks/length")
      complete(service.getLength(databaseName, tableName))
    }
  }

  /**
   * Database Table Range-specific API routes (e.g. "/portfolio/stocks/187/23")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param start        the start of the range
   * @param length       the number of rows referenced
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRange(databaseName: String, tableName: String, start: Int, length: Int)(implicit service: ServerSideTableService): Route = {
    delete {
      // delete the range of rows (e.g. "DELETE /portfolio/stocks/287/20")
      complete(service.deleteRange(databaseName, tableName, start, length))
    } ~
      get {
        // retrieve the range of rows (e.g. "GET /portfolio/stocks/287/20")
        complete(service.getRange(databaseName, tableName, start, length).map(_.toMap))
      }
  }

  /**
   * Database Table SQL-specific API routes (e.g. "/portfolio/stocks/sql")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableSQL(databaseName: String, tableName: String)(implicit service: ServerSideTableService): Route = {
    post {
      // execute the SQL query (e.g. "POST /portfolio" <~ { sql: "TRUNCATE TABLE staging" })
      entity(as[String]) { sql =>
        complete(service.executeQuery(databaseName, tableName, sql).map(_.toMap))
      }
    }
  }

  /**
   * Database Table Length-specific API routes (e.g. "/portfolio/stocks/187")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the referenced row ID
   * @param service      the implicit [[ServerSideTableService]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRowID(databaseName: String, tableName: String, rowID: Int)(implicit service: ServerSideTableService): Route = {
    delete {
      // delete the row by ID (e.g. "DELETE /portfolio/stocks/129")
      complete(service.deleteRow(databaseName, tableName, rowID))
    } ~
      get {
        // retrieve the row by ID (e.g. "GET /portfolio/stocks/287")
        service.getRow(databaseName, tableName, rowID) match {
          case Some(row) => complete(row.toMap.toJson)
          case None => complete(JsObject())
        }
      } ~
      post {
        // partially update the row by ID
        // (e.g. "POST /portfolio/stocks/287" <~ { "lastSale":2.23, "lastSaleTime":1596404391000 })
        entity(as[JsObject]) { jsObject =>
          complete(service.updateRow(databaseName, tableName, rowID, toValues(jsObject)))
        }
      } ~
      put {
        // replace the row by ID
        // (e.g. "PUT /portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(service.replaceRow(databaseName, tableName, rowID, toValues(jsObject)))
        }
      }
  }

  private def toValues(params: Uri.Query): TupleSet = Map(params.filterNot(_._1.name.startsWith("__")): _*)

  private def toValues(jsObject: JsObject): TupleSet = jsObject.fields.map { case (k, js) => (k, js.unwrapJSON) }

}
