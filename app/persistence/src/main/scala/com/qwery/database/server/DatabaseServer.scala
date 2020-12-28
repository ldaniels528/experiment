package com.qwery.database
package server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, _}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import com.qwery.database.ColumnTypes.{ArrayType, BlobType, ClobType, ColumnType, StringType}
import com.qwery.database.JSONSupport._
import com.qwery.database.models._
import com.qwery.database.server.DatabaseJsonProtocol._
import com.qwery.models.expressions.Expression
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
 * Qwery Database Server
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
    logger.info(f"Qwery Database Server v$version%.1f")

    // get the bind/listen port
    val port = args match {
      case Array(port, _*) => port.toInt
      case Array() => defaultPort
    }

    // create the actor pool
    implicit val system: ActorSystem = ActorSystem(name = "database-server")
    implicit val queryProcessor: QueryProcessor = new QueryProcessor(requestTimeout = 2.minutes)
    import system.dispatcher

    // start the server
    startServer(port = port)
  }

  /**
   * Starts the server
   * @param host the server host (e.g. "0.0.0.0")
   * @param port the server port
   * @param as   the implicit [[ActorSystem]]
   * @param ec   the implicit [[ExecutionContext]]
   * @param qp   the implicit [[QueryProcessor]]
   */
  def startServer(host: String = "0.0.0.0", port: Int)(implicit as: ActorSystem, ec: ExecutionContext, qp: QueryProcessor): Unit = {
    Http().newServerAt(host, port).bindFlow(route()) onComplete {
      case Success(serverBinding) =>
        logger.info(s"listening to ${serverBinding.localAddress}")
      case Failure(e) =>
        logger.error(s"Error: ${e.getMessage}", e)
    }
  }

  /**
   * Define the API routes
   * @param ec the implicit [[ExecutionContext]]
   * @param qp the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  def route()(implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    // route: / (root)
    pathSingleSlash(routesRoot) ~
      // route: /c/<database>?table=<value>&column=<value> (e.g. "/c/portfolio?table=stock&column=symbol")
      path("c" / Segment)(routesByColumns) ~
      // route: /d/<database> (e.g. "/d/portfolio")
      path("d" / Segment)(routesByDatabase) ~
      // route: /d/<database>/<table> (e.g. "/d/portfolio/stocks")
      path("d" / Segment / Segment)(routesByDatabaseTable) ~
      // route: /d/<database>/<table>/<rowID> (e.g. "/d/portfolio/stocks/187")
      path("d" / Segment / Segment / IntNumber)(routesByDatabaseTableRowID) ~
      // route: /d/<database>/<table>/<rowID>/<columnID> (e.g. "/d/portfolio/stocks/187/2")
      path("d" / Segment / Segment / IntNumber / IntNumber)(routesByDatabaseTableColumnID) ~
      // route: /r/<database>/<table>/<rowID>/<count> (e.g. "/r/portfolio/stocks/187/23")
      path("r" / Segment / Segment / IntNumber / IntNumber)(routesByDatabaseTableRange) ~
      // route: /d/<database>/<table>/export/<format>/<fileName> (e.g. "/d/portfolio/stocks/export/json/stocks.json")
      path("d" / Segment / Segment / "export" / Segment / Segment)(routesByDatabaseTableExport) ~
      // route: /d/<database>/<table>/length (e.g. "/d/portfolio/stocks/length")
      path("d" / Segment / Segment / "length")(routesByDatabaseTableLength) ~
      // route: /q/<database> (e.g. "/q/portfolio")
      path("q" / Segment)(routesByDatabaseTableQuery)
  }

  /**
    * Database Root API routes (e.g. "/")
    * @param qp the implicit [[QueryProcessor]]
    * @return the [[Route]]
    */
  private def routesRoot(implicit qp: QueryProcessor): Route = {
    get {
      // retrieve the list of databases (e.g. "GET /")
      complete(qp.getDatabases)
    }
  }

  /**
   * Database-specific API routes (e.g. "/d/portfolio")
   * @param databaseName the name of the database
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabase(databaseName: String)(implicit qp: QueryProcessor): Route = {
    get {
      // retrieve the database metrics (e.g. "GET /d/portfolio")
      complete(qp.getDatabaseMetrics(databaseName))
    } ~
      post {
        // create the new table (e.g. "POST /d/portfolio/stocks" <~ {"tableName":"stocks_client_test_0", "columns":[...]}
        entity(as[String]) { jsonString =>
          val ref = jsonString.fromJSON[TableCreation]
          complete(qp.createTable(databaseName, ref.tableName, ref.columns))
        }
      }
  }

  /**
    * Database Column-search API routes (e.g. "/c/shocktrade?table=stock&column=symbol")
    * @param databaseName the name of the database
    * @param ec           the implicit [[ExecutionContext]]
    * @param qp           the implicit [[QueryProcessor]]
    * @return the [[Route]]
    */
  private def routesByColumns(databaseName: String)(implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    get {
      // retrieve the table metrics (e.g. "GET /c/shocktrade?table=stock&column=symbol")
      extract(_.request.uri.query()) { params =>
        val (tablePattern_?, columnPattern_?) = (params.get("table"), params.get("column"))
        complete(qp.getColumns(databaseName, tablePattern_?, columnPattern_?))
      }
    }
  }

  /**
   * Database Table-specific API routes (e.g. "/d/portfolio/stocks")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param ec           the implicit [[ExecutionContext]]
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTable(databaseName: String, tableName: String)(implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    delete {
      // drop the table by name (e.g. "DELETE /d/portfolio/stocks")
      complete(qp.dropTable(databaseName, tableName, ifExists = true))
    } ~
      get {
        // retrieve the table metrics (e.g. "GET /d/portfolio/stocks")
        // or query via query parameters (e.g. "GET /d/portfolio/stocks?exchange=AMEX&__limit=5")
        extract(_.request.uri.query()) { params =>
          val (limit, condition) = (params.get("__limit").map(_.toInt), toValues(params))
          complete(
            if (params.isEmpty) qp.getTableMetrics(databaseName, tableName)
            else qp.findRows(databaseName, tableName, condition, limit).map(_.map(_.toKeyValues))
          )
        }
      } ~
      post {
        // append the new record to the table
        // (e.g. "POST /d/portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(qp.insertRow(databaseName, tableName, toValues(jsObject)))
        }
      }
  }

  /**
   * Database Table Export-specific API routes (e.g. "/d/portfolio/stocks/export/csv/stocks.csv")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param format       the destination file format (e.g. "csv", "json", "bin")
   * @param fileName     the name of the file to be downloaded
   * @return the [[Route]]
   */
  private def routesByDatabaseTableExport(databaseName: String, tableName: String, format: String, fileName: String): Route = {
    val dataFile = format.toLowerCase() match {
      case "bin" | "binary" | "raw" =>
        TableFile.getTableDataFile(databaseName, tableName)
      case "csv" =>
        val csvFile = createTempFile()
        TableFile.getTableFile(databaseName, tableName).exportAsCSV(csvFile)
        csvFile
      case "json" =>
        val jsonFile = createTempFile()
        TableFile.getTableFile(databaseName, tableName).exportAsJSON(jsonFile)
        jsonFile
      case other =>
        die(s"Unsupported file format '$other'")
    }
    logger.info(s"Exporting '$fileName' (as ${format.toUpperCase()}) <~ ${dataFile.getAbsolutePath}")
    getFromFile(dataFile)
  }

  /**
   * Database Table Length-specific API routes (e.g. "/d/portfolio/stocks/length")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableLength(databaseName: String, tableName: String)(implicit qp: QueryProcessor): Route = {
    get {
      // retrieve the length of the table (e.g. "GET /d/portfolio/stocks/length")
      complete(qp.getTableLength(databaseName, tableName))
    }
  }

  /**
   * Database Table Field-specific API routes (e.g. "/d/portfolio/stocks/187/0")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the desired row by ID
   * @param columnID     the desired column by index
   * @param ec           the implicit [[ExecutionContext]]
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableColumnID(databaseName: String, tableName: String, rowID: ROWID, columnID: Int)
                                           (implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    delete {
      // delete a field (e.g. "DELETE /d/portfolio/stocks/287/0")
      complete(qp.deleteField(databaseName, tableName, rowID, columnID))
    } ~
      get {
        // retrieve a field (e.g. "GET /d/portfolio/stocks/287/0" ~> "CAKE")
        parameters('__contentType.?) { contentType_? =>
          val column = TableFile.getTableFile(databaseName, tableName).device.columns(columnID)
          val contentType = contentType_?.map(toContentType).getOrElse(toContentType(column.metadata.`type`))
          complete(qp.getField(databaseName, tableName, rowID, columnID) map { field =>
            val fieldBytes = field.typedValue.encode(column)
            if (fieldBytes.length <= FieldMetadata.BYTES_LENGTH) HttpResponse(status = StatusCodes.NoContent)
            else {
              // copy the field's contents only (without metadata)
              val content = new Array[Byte](fieldBytes.length - FieldMetadata.BYTES_LENGTH)
              System.arraycopy(fieldBytes, FieldMetadata.BYTES_LENGTH, content, 0, content.length)
              val response = HttpResponse(StatusCodes.OK, headers = Nil, entity = HttpEntity(contentType, content))
              response.entity.withContentType(contentType).withSizeLimit(content.length)
              response
            }
          })
        }
      } ~
      put {
        // updates a field (e.g. "PUT /d/portfolio/stocks/287/3" <~ 124.56)
        entity(as[String]) { value =>
          val device = TableFile.getTableFile(databaseName, tableName).device
          assert(device.columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
          val columnType = device.columns(columnID).metadata.`type`
          complete(qp.updateField(databaseName, tableName, rowID, columnID, Option(Codec.convertTo(value, columnType))))
        }
      }
  }

  /**
   * Database Table Range-specific API routes (e.g. "/r/portfolio/stocks/187/23")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param start        the start of the range
   * @param length       the number of rows referenced
   * @param ec           the implicit [[ExecutionContext]]
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRange(databaseName: String, tableName: String, start: ROWID, length: Int)
                                        (implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    delete {
      // delete the range of rows (e.g. "DELETE /r/portfolio/stocks/287/20")
      complete(qp.deleteRange(databaseName, tableName, start, length))
    } ~
      get {
        // retrieve the range of rows (e.g. "GET /r/portfolio/stocks/287/20")
        complete(qp.getRange(databaseName, tableName, start, length).map(_.map(_.toKeyValues)))
      }
  }

  /**
   * Database Table Query-specific API routes (e.g. "/q/portfolio")
   * @param databaseName the name of the database
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableQuery(databaseName: String)(implicit qp: QueryProcessor): Route = {
    post {
      // execute the SQL query (e.g. "POST /d/portfolio" <~ "TRUNCATE TABLE staging")
      entity(as[String]) { sql =>
        logger.info(sql)
        complete(qp.executeQuery(databaseName, sql))
      }
    }
  }

  /**
   * Database Table Length-specific API routes (e.g. "/portfolio/stocks/187")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the referenced row ID
   * @param ec           the implicit [[ExecutionContext]]
   * @param qp           the implicit [[QueryProcessor]]
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRowID(databaseName: String, tableName: String, rowID: Int)
                                        (implicit ec: ExecutionContext, qp: QueryProcessor): Route = {
    delete {
      // delete the row by ID (e.g. "DELETE /d/portfolio/stocks/129")
      complete(qp.deleteRow(databaseName, tableName, rowID))
    } ~
      get {
        // retrieve the row by ID (e.g. "GET /d/portfolio/stocks/287")
        parameters('__metadata.?) { metadata_? =>
          val isMetadata = metadata_?.map(_.toLowerCase).contains("true")
          complete(qp.getRow(databaseName, tableName, rowID) map {
            case Some(row) => if (isMetadata) row.toLiftJs.toSprayJs else row.toKeyValues.toJson
            case None => JsObject()
          })
        }
      } ~
      post {
        // partially update the row by ID
        // (e.g. "POST /d/portfolio/stocks/287" <~ { "lastSale":2.23, "lastSaleTime":1596404391000 })
        entity(as[JsObject]) { jsObject =>
          complete(qp.updateRow(databaseName, tableName, rowID, toExpressions(jsObject)))
        }
      } ~
      put {
        // replace the row by ID
        // (e.g. "PUT /d/portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(qp.replaceRow(databaseName, tableName, rowID, toValues(jsObject)))
        }
      }
  }

  private def toContentType(`type`: String): ContentType = {
    `type`.toLowerCase() match {
      case "binary" => ContentTypes.`application/octet-stream`
      case "csv" => ContentTypes.`text/csv(UTF-8)`
      case "json" => ContentTypes.`application/json`
      case "html" => ContentTypes.`text/html(UTF-8)`
      case "text" => ContentTypes.`text/plain(UTF-8)`
      case "xml" => ContentTypes.`text/xml(UTF-8)`
      case _ => toContentType(StringType)
    }
  }

  private def toContentType(`type`: ColumnType): ContentType = {
    `type` match {
      case ArrayType | BlobType => ContentTypes.`application/octet-stream`
      case ClobType | StringType => ContentTypes.`text/html(UTF-8)`
      case _ => ContentTypes.`text/plain(UTF-8)`
    }
  }

  private def toValues(params: Uri.Query): KeyValues = KeyValues(params.filterNot(_._1.name.startsWith("__")): _*)

  private def toValues(jsObject: JsObject): KeyValues = KeyValues(jsObject.fields.map { case (k, js) => (k, js.unwrapJSON) })

  private def toExpressions(jsObject: JsObject): Seq[(String, Expression)] = {
    jsObject.fields.toSeq.map { case (k, js) => (k, js.toExpression) }
  }

}
