package com.qwery.database
package server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, _}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import com.qwery.database.models.ColumnTypes.{ArrayType, BlobType, ClobType, ColumnType, StringType}
import com.qwery.database.device.BlockDevice
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.DatabaseManagementSystem._
import com.qwery.database.files.{DatabaseManagementSystem, TableFile}
import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.models.QueryResult.SolutionToQueryResult
import com.qwery.database.models._
import com.qwery.database.server.DatabaseServer.implicits._
import com.qwery.database.util.Codec
import com.qwery.models.expressions.Expression
import com.qwery.util.OptionHelper.OptionEnrichment
import org.slf4j.LoggerFactory
import spray.json._

import java.text.NumberFormat
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Qwery Database Server
 */
object DatabaseServer {
  private val logger = LoggerFactory.getLogger(getClass)
  private val nf = NumberFormat.getNumberInstance
  private val cpu = new DatabaseCPU()
  private val pidGenerator = new AtomicLong()

  /**
   * Main program
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {
    val defaultPort = 8233

    // display the application version
    val version = 0.1
    logger.info(f"Qwery Database Server v$version%.1f")

    // get the configuration
    val port: Int = args.toList match {
      case port :: _ => port.toInt
      case _ => defaultPort
    }

    // create the actor pool
    implicit val system: ActorSystem = ActorSystem(name = "database-server")
    import system.dispatcher

    // start the server
    startServer(port = port)
  }

  /**
   * Starts the server
   * @param host the server host (e.g. "0.0.0.0")
   * @param port the server port
   */
  def startServer(host: String = "0.0.0.0", port: Int)(implicit as: ActorSystem, ec: ExecutionContext): Unit = {
    Http().newServerAt(host, port).bindFlow(route(host, port)) onComplete {
      case Success(serverBinding) =>
        logger.info(s"listening to ${serverBinding.localAddress}")
      case Failure(e) =>
        logger.error(s"Error: ${e.getMessage}", e)
    }
  }

  /**
   * Define the API routes
   * @return the [[Route]]
   */
  def route(host: String, port: Int): Route = {
    // Database API routes
    // route: /d/<database> (e.g. "/d/portfolio")
    path("d" / Segment)(routesByDatabase(_, host, port)) ~
      // route: /d/<database>/<table> (e.g. "/d/portfolio/stocks")
      path("d" / Segment / Segment)(routesByDatabaseTable) ~
      // route: /d/<database>/<table>/<rowID> (e.g. "/d/portfolio/stocks/187")
      path("d" / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      // route: /d/<database>/<table>/<rowID>/<columnID> (e.g. "/d/portfolio/stocks/187/2")
      path("d" / Segment / Segment / LongNumber / IntNumber)(routesByDatabaseTableColumnID) ~
      // route: /r/<database>/<table>/<rowID>/<count> (e.g. "/r/portfolio/stocks/187/23")
      path("r" / Segment / Segment / LongNumber / IntNumber)(routesByDatabaseTableRange) ~
      // route: /d/<database>/<table>/export/<format>/<fileName> (e.g. "/d/portfolio/stocks/export/json/stocks.json")
      path("d" / Segment / Segment / "export" / Segment / Segment)(routesByDatabaseTableExport) ~
      // route: /d/<database>/<table>/length (e.g. "/d/portfolio/stocks/length")
      path("d" / Segment / Segment / "length")(routesByDatabaseTableLength) ~
      // route: /q/<database> (e.g. "/q/portfolio")
      path("q" / Segment)(routesByDatabaseTableQuery) ~
      //
      // Message API routes
      // route: /m/<database>/<table> (e.g. "/m/portfolio/stocks" <~ """{ "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 }""")
      path("m" / Segment / Segment)(routesMessaging) ~
      // route: /m/<database>/<table>/<rowID> (e.g. "/d/portfolio/stocks/187")
      path("m" / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      //
      // Search API routes
      // route: /columns/?database=<pattern>&table=<pattern>&column=<pattern> (e.g. "/columns/?database=portfolios&table=stocks&column=symbol")
      path("columns")(routesSearchColumns) ~
      // route: /databases/?database=<pattern> (e.g. "/databases/?database=test%")
      path("databases")(routesSearchDatabases) ~
      // route: /tables/?database=<pattern>&table=<pattern> (e.g. "/tables/?database=test%&table=stocks")
      path("tables")(routesSearchTables)
  }

  /**
   * Database-specific API routes (e.g. "/d/portfolio")
   * @param databaseName the name of the database
   * @return the [[Route]]
   */
  private def routesByDatabase(databaseName: String, host: String, port: Int): Route = {
    get {
      // retrieve the database summary (e.g. "GET /d/portfolio")
      val databaseSummary = DatabaseManagementSystem.getDatabaseSummary(databaseName)
      val databaseSummaryWithRefs = databaseSummary.copy(tables = databaseSummary.tables.map { ts =>
        ts.copy(href = Some(s"http://$host:$port/d/$databaseName/${ts.tableName}"))
      })
      complete(databaseSummaryWithRefs)
    }
  }

  /**
   * Database Table-specific API routes (e.g. "/d/portfolio/stocks")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesByDatabaseTable(databaseName: String, tableName: String): Route = {
    delete {
      // drop the table by name (e.g. "DELETE /d/portfolio/stocks")
      complete(cpu.dropTable(databaseName, tableName, ifExists = true).toUpdateCount(1))
    } ~
      get {
        // retrieve the table metrics (e.g. "GET /d/portfolio/stocks")
        // or query via query parameters (e.g. "GET /d/portfolio/stocks?exchange=AMEX&__limit=5")
        extract(_.request.uri.query()) { params =>
          val (limit, condition) = (params.get("__limit").map(_.toInt) ?? Some(20), toValues(params))
          if (params.isEmpty) complete(cpu.getTableMetrics(databaseName, tableName))
          else complete(cpu.getRows(databaseName, tableName, condition, limit).toList.map(_.toKeyValues))
        }
      } ~
      post {
        // append the new record to the table
        // (e.g. "POST /d/portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.insertRow(databaseName, tableName, toExpressions(jsObject)).toUpdateCount(1))
        }
      } ~
      put {
        // create the new table (e.g. "PUT /d/portfolio/stocks" <~ {"tableName":"stocks_client_test_0", "columns":[...]})
        entity(as[TableProperties]) { properties =>
          complete(cpu.createTable(databaseName, tableName, properties).toUpdateCount(1))
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
        getTableDataFile(databaseName, tableName)
      case "csv" =>
        val csvFile = createTempFile()
        TableFile(databaseName, tableName).exportAsCSV(csvFile)
        csvFile
      case "json" =>
        val jsonFile = createTempFile()
        TableFile(databaseName, tableName).exportAsJSON(jsonFile)
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
   * @return the [[Route]]
   */
  private def routesByDatabaseTableLength(databaseName: String, tableName: String): Route = {
    get {
      // retrieve the length of the table (e.g. "GET /d/portfolio/stocks/length")
      complete(cpu.getTableLength(databaseName, tableName).toUpdateCount)
    }
  }

  /**
   * Database Table Field-specific API routes (e.g. "/d/portfolio/stocks/187/0")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the desired row by ID
   * @param columnID     the desired column by index
   * @return the [[Route]]
   */
  private def routesByDatabaseTableColumnID(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Route = {
    delete {
      // delete a field (e.g. "DELETE /d/portfolio/stocks/287/0")
      complete(cpu.deleteField(databaseName, tableName, rowID, columnID).toUpdateCount(1))
    } ~
      get {
        // retrieve a field (e.g. "GET /d/portfolio/stocks/287/0" ~> "CAKE")
        parameters('__contentType.?) { contentType_? =>
          val columns = cpu.getColumns(databaseName, tableName)
          val column = columns(columnID)
          val contentType = contentType_?.map(toContentType).getOrElse(toContentType(column.metadata.`type`))
          val field = cpu.getField(databaseName, tableName, rowID, columnID)
          val fieldBytes = field.typedValue.encode(column)
          complete({
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
          val columns = cpu.getColumns(databaseName, tableName)
          assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
          val columnType = columns(columnID).metadata.`type`
          complete(cpu.updateField(databaseName, tableName, rowID, columnID, Option(Codec.convertTo(value, columnType))).toUpdateCount(1))
        }
      }
  }

  /**
   * Database Table Range-specific API routes (e.g. "/r/portfolio/stocks/187/23")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param start        the start of the range
   * @param length       the number of rows referenced
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRange(databaseName: String, tableName: String, start: ROWID, length: Int): Route = {
    delete {
      // delete the range of rows (e.g. "DELETE /r/portfolio/stocks/287/20")
      complete(cpu.deleteRange(databaseName, tableName, start, length).toUpdateCount)
    } ~
      get {
        // retrieve the range of rows (e.g. "GET /r/portfolio/stocks/287/20")
        complete(cpu.getRange(databaseName, tableName, start, length).toList.map(_.toKeyValues))
      }
  }

  /**
   * Database Table Query-specific API routes (e.g. "/q/portfolio")
   * @param databaseName the name of the database
   * @return the [[Route]]
   */
  private def routesByDatabaseTableQuery(databaseName: String): Route = {
    post {
      // execute the SQL query (e.g. "POST /d/portfolio" <~ "TRUNCATE TABLE staging")
      entity(as[String]) { sql =>
        val pid = pidGenerator.addAndGet(1)
        val startTime = System.nanoTime()
        logger.info(f"[$pid%04d] SQL: $sql")

        try {
          cpu.executeQuery(databaseName, sql).map(_.toQueryResult) match {
            case Some(results) =>
              val elapsedTime = (System.nanoTime() - startTime) / 1e+6
              logger.info(f"[$pid%04d] ${nf.format(results.rows.length)} results returned in $elapsedTime%.1f msec")
              results.show(5)(logger)
              complete(results)
            case None =>
              complete(QueryResult(databaseName, tableName = ""))
          }
        } catch {
          case e: Exception =>
            logger.error(f"[$pid%04d] ${e.getMessage}")
            complete(StatusCodes.InternalServerError.copy()(reason = e.getMessage, defaultMessage = e.getMessage))
        }
      }
    }
  }

  /**
   * Database Table Length-specific API routes (e.g. "/portfolio/stocks/187")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @param rowID        the referenced row ID
   * @return the [[Route]]
   */
  private def routesByDatabaseTableRowID(databaseName: String, tableName: String, rowID: ROWID): Route = {
    delete {
      // delete the row by ID (e.g. "DELETE /d/portfolio/stocks/129")
      complete(cpu.deleteRow(databaseName, tableName, rowID).toUpdateCount(1))
    } ~
      get {
        // retrieve the row by ID (e.g. "GET /d/portfolio/stocks/287")
        complete(cpu.getRow(databaseName, tableName, rowID) match {
          case Some(row) => row.toKeyValues.toJson
          case None => JsObject()
        })
      } ~
      post {
        // partially update the row by ID
        // (e.g. "POST /d/portfolio/stocks/287" <~ { "lastSale":2.23, "lastSaleTime":1596404391000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.updateRow(databaseName, tableName, rowID, toExpressions(jsObject)).toUpdateCount(1))
        }
      } ~
      put {
        // replace the row by ID
        // (e.g. "PUT /d/portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.replaceRow(databaseName, tableName, rowID, toValues(jsObject)).toUpdateCount(1))
        }
      }
  }

  /**
   * Messaging-specific API routes (e.g. "/m/portfolio/stocks")
   * @param databaseName the name of the database
   * @param tableName    the name of the table
   * @return the [[Route]]
   */
  private def routesMessaging(databaseName: String, tableName: String): Route = {
    post {
      // append the new message to the table
      // (e.g. "POST /m/portfolio/stocks" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
      entity(as[JsObject]) { jsObject =>
        complete(cpu.insertRow(databaseName, tableName, toExpressions(jsObject)).toUpdateCount(1))
      }
    }
  }

  /**
    * Column search API routes (e.g. "/columns?database=shocktrade&table=stock%&column=symbol")
    * @return the [[Route]]
    */
  private def routesSearchColumns: Route = {
    get {
      // search for columns (e.g. "GET /columns?database=shocktrade&table=stock%&column=symbol")
      extract(_.request.uri.query()) { params =>
        val (databasePattern_?, tablePattern_?, columnPattern_?) = (params.get("database"), params.get("table"), params.get("column"))
        complete(searchColumns(databasePattern_?, tablePattern_?, columnPattern_?))
      }
    }
  }

  /**
    * Database search API routes (e.g. "/databases?database=test%")
    * @return the [[Route]]
    */
  private def routesSearchDatabases: Route = {
    get {
      // search for databases (e.g. "GET /databases?database=test%")
      extract(_.request.uri.query()) { params =>
        val databasePattern_? = params.get("database")
        complete(searchDatabases(databasePattern_?))
      }
    }
  }

  /**
    * Table search API routes (e.g. "/tables?database=test%&table=stock%")
    * @return the [[Route]]
    */
  private def routesSearchTables: Route = {
    get {
      // search for tables (e.g. "GET /tables?database=test%&table=stock%"")
      extract(_.request.uri.query()) { params =>
        val (databasePattern_?, tablePattern_?) = (params.get("database"), params.get("table"))
        complete(searchTables(databasePattern_?, tablePattern_?))
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

  /**
    * implicits definitions
    */
  object implicits {

    /**
      * Results Conversion
      * @param response the host response
      */
    final implicit class ResultsConversion(val response: Any) extends AnyVal {

      @inline
      def toUpdateCount(count: Long): UpdateCount = UpdateCount(count)

      @inline
      def toUpdateCount: UpdateCount = response match {
        case bool: Boolean => UpdateCount(bool)
        case count: Long => UpdateCount(count)
        case unknown =>
          logger.warn(s"Cannot convert '$unknown' (${unknown.getClass.getName}) to ${classOf[UpdateCount].getName}")
          UpdateCount(1)
      }
    }

    /**
      * Response Conversion
      * @param response the response
      */
    final implicit class ResponseConversion(val response: Either[BlockDevice, Long]) extends AnyVal {

      @inline
      def asResult(databaseName: String, tableName: String, limit: Option[Int]): QueryResult = response match {
        case Left(device) =>
          var rows: List[Seq[Option[Any]]] = Nil
          device.whileRow(KeyValues(), limit ?? Some(20)) { row => rows = row.fields.map(_.value) :: rows }
          QueryResult(databaseName, tableName, columns = device.columns, rows = rows)
        case Right(count) =>
          QueryResult(databaseName, tableName, columns = Nil, count = count)
      }

      @inline
      def asUpdate: UpdateCount = response match {
        case Left(device) => UpdateCount(device.length)
        case Right(count) => UpdateCount(count)
      }

    }

  }

}
