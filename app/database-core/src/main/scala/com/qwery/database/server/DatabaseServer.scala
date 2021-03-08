package com.qwery.database
package server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpResponse, _}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.directives.ContentTypeResolver.Default
import com.qwery.database.device.BlockDevice
import com.qwery.database.files.DatabaseFiles._
import com.qwery.database.files.DatabaseManagementSystem._
import com.qwery.database.files.{DatabaseManagementSystem, TableFile}
import com.qwery.database.models.ColumnTypes.{ArrayType, BlobType, ClobType, ColumnType, StringType}
import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.models.QueryResult.SolutionToQueryResult
import com.qwery.database.models._
import com.qwery.database.server.DatabaseServer.implicits._
import com.qwery.database.util.Codec
import com.qwery.models.expressions.Expression
import com.qwery.models.{Table, EntityRef}
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
  private implicit val scope: Scope = Scope()

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
    * Starts the server; listens to 0.0.0.0:port
    * @param port the server port
    */
  def startServer(port: Int)(implicit as: ActorSystem, ec: ExecutionContext): Unit = {
    startServer(host = "0.0.0.0", port = port)
  }

  /**
    * Starts the server
    * @param host the server host (e.g. "0.0.0.0")
    * @param port the server port
    */
  def startServer(host: String, port: Int)(implicit as: ActorSystem, ec: ExecutionContext): Unit = {
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
      // route: /d/<database>/<schema> (e.g. "/d/portfolio/stocks")
      path("d" / Segment / Segment)(routesByDatabaseSchema(_, _, host, port)) ~
      // route: /d/<database>/<schema>/<table> (e.g. "/d/portfolio/stocks/nasdaq")
      path("d" / Segment / Segment / Segment)(routesByDatabaseTable) ~
      // route: /d/<database>/<schema>/<table>/<rowID> (e.g. "/d/portfolio/stocks/nasdaq/187")
      path("d" / Segment / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      // route: /d/<database>/<schema>/<table>/<rowID>/<columnID> (e.g. "/d/portfolio/stocks/nasdaq/187/2")
      path("d" / Segment / Segment / Segment / LongNumber / IntNumber)(routesByDatabaseTableColumnID) ~
      // route: /r/<database>/<schema>/<table>/<rowID>/<count> (e.g. "/r/portfolio/stocks/nasdaq/187/23")
      path("r" / Segment / Segment / Segment / LongNumber / IntNumber)(routesByDatabaseTableRange) ~
      // route: /d/<database>/<schema>/<table>/export/<format>/<fileName> (e.g. "/d/portfolio/stocks/nasdaq/export/json/stocks.json")
      path("d" / Segment / Segment / Segment / "export" / Segment / Segment)(routesByDatabaseTableExport) ~
      // route: /d/<database>/<schema>/<table>/length (e.g. "/d/portfolio/stocks/nasdaq/length")
      path("d" / Segment / Segment / Segment / "length")(routesByDatabaseTableLength) ~
      // route: /q/<database> (e.g. "/q/portfolio")
      path("q" / Segment)(routesByDatabaseQuery) ~
      //
      // Message API routes
      // route: /m/<database>/<schema>/<table> (e.g. "/m/portfolio/stocks/otc" <~ """{ "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 }""")
      path("m" / Segment / Segment / Segment)(routesMessaging) ~
      // route: /m/<database>/<schema>/<table>/<rowID> (e.g. "/d/portfolio/stocks/otc/187")
      path("m" / Segment / Segment / Segment / LongNumber)(routesByDatabaseTableRowID) ~
      //
      // Search API routes
      // route: /columns/?database=<pattern>&table=<pattern>&column=<pattern> (e.g. "/columns/?database=portfolios&table=stocks&column=symbol")
      path("columns")(routesSearchColumns) ~
      // route: /databases/?database=<pattern> (e.g. "/databases/?database=test%")
      path("databases")(routesSearchDatabases) ~
      // route: /schemas/?database=<pattern>&schema=<pattern> (e.g. "/schemas/?database=test%&schema=stocks")
      path("schemas")(routesSearchSchemas) ~
      // route: /tables/?database=<pattern>&schema=<pattern&table=<pattern> (e.g. "/tables/?database=test%&table=stocks")
      path("tables")(routesSearchTables)
  }

  /**
    * Database-specific API routes (e.g. "/d/portfolio")
    * @param databaseName the name of the database
    * @param host         the server host
    * @param port         the server port
    * @return the [[Route]]
    */
  def routesByDatabase(databaseName: String, host: String, port: Int): Route = {
    get {
      // retrieve the database summary (e.g. "GET /d/portfolio")
      val databaseSummary = DatabaseManagementSystem.getDatabaseSummary(databaseName)
      val databaseSummaryWithRefs = databaseSummary.copy(tables = databaseSummary.tables.map { ts =>
        ts.copy(href = Some(ts.toURL(databaseName, host, port)))
      })
      complete(databaseSummaryWithRefs)
    }
  }

  /**
    * Database Query-specific API routes (e.g. "/q/portfolio")
    * @param databaseName the name of the database
    * @return the [[Route]]
    */
  def routesByDatabaseQuery(databaseName: String): Route = {
    post {
      // execute the SQL query (e.g. "POST /d/portfolio" <~ "TRUNCATE TABLE staging")
      entity(as[String]) { sql =>
        val pid = pidGenerator.addAndGet(1)
        val startTime = System.nanoTime()
        logger.info(f"[$pid%05d] SQL: ${sql.replaceAllLiterally("\n", " ").replaceAllLiterally("  ", " ")}")

        try {
          cpu.executeQuery(databaseName, sql).map(_.toQueryResult) match {
            case Some(results) =>
              val elapsedTime = (System.nanoTime() - startTime) / 1e+6
              logger.info(f"[$pid%05d] ${nf.format(results.rows.length)} results returned in $elapsedTime%.1f msec")
              results.show(5)(logger)
              complete(results)
            case None =>
              complete(QueryResult(EntityRef.parse("???")))
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
    * Database Table-specific API routes (e.g. "/d/portfolio/stocks")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param host         the server host
    * @param port         the server port
    * @return the [[Route]]
    */
  def routesByDatabaseSchema(databaseName: String, schemaName: String, host: String, port: Int): Route = {
    get {
      // retrieve the database summary by schema (e.g. "GET /d/portfolio/stocks")
      val databaseSummary = DatabaseManagementSystem.getDatabaseSummary(databaseName, Option(schemaName))
      val databaseSummaryWithRefs = databaseSummary.copy(tables = databaseSummary.tables.map { ts =>
        ts.copy(href = Some(ts.toURL(databaseName, host, port)))
      })
      complete(databaseSummaryWithRefs)
    } ~
      post {
        // create a new table (e.g. "POST /d/portfolio/stocks" <~ {"ref":{"databaseName":"test","schemaName":"stocks","tableName":"stocks_jdbc"}, "columns":[...]})
        entity(as[Table]) { table =>
          complete(cpu.createTable(table).toUpdateCount(1))
        }
      }
  }

  /**
    * Database Table-specific API routes (e.g. "/d/portfolio/stocks/amex")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @return the [[Route]]
    */
  def routesByDatabaseTable(databaseName: String, schemaName: String, tableName: String): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    delete {
      // drop the table by name (e.g. "DELETE /d/portfolio/stocks/amex")
      complete(cpu.dropTable(ref, ifExists = true).toUpdateCount(1))
    } ~
      get {
        // retrieve the table metrics (e.g. "GET /d/portfolio/stocks/amex")
        // or query via query parameters (e.g. "GET /d/portfolio/stocks/amex?exchange=AMEX&__limit=5")
        extract(_.request.uri.query()) { params =>
          val (limit, condition) = (params.get("__limit").map(_.toInt) ?? Some(20), toValues(params))
          if (params.isEmpty) complete(cpu.getTableMetrics(ref))
          else complete(cpu.getRows(ref, condition, limit).toList.map(_.toKeyValues))
        }
      } ~
      post {
        // append the new record to the table
        // (e.g. "POST /d/portfolio/stocks/amex" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.insertRow(ref, toExpressions(jsObject)).toUpdateCount(1))
        }
      }
  }

  /**
    * Database Table Export-specific API routes (e.g. "/d/portfolio/stocks/amex/export/csv/stocks.csv")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @param format       the destination file format (e.g. "csv", "json", "bin")
    * @param fileName     the name of the file to be downloaded
    * @return the [[Route]]
    */
  def routesByDatabaseTableExport(databaseName: String, schemaName: String, tableName: String, format: String, fileName: String): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    val dataFile = format.toLowerCase() match {
      case "bin" | "binary" | "raw" =>
        getTableDataFile(ref, config = readTableConfig(ref))
      case "csv" =>
        val csvFile = createTempFile()
        TableFile(ref).exportAsCSV(csvFile)
        csvFile
      case "json" =>
        val jsonFile = createTempFile()
        TableFile(ref).exportAsJSON(jsonFile)
        jsonFile
      case other =>
        die(s"Unsupported file format '$other'")
    }
    logger.info(s"Exporting '$fileName' (as ${format.toUpperCase()}) <~ ${dataFile.getAbsolutePath}")
    getFromFile(dataFile)
  }

  /**
    * Database Table Length-specific API routes (e.g. "/d/portfolio/stocks/amex/length")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @return the [[Route]]
    */
  def routesByDatabaseTableLength(databaseName: String, schemaName: String, tableName: String): Route = {
    get {
      // retrieve the length of the table (e.g. "GET /d/portfolio/stocks/length")
      complete(cpu.getTableLength(new EntityRef(databaseName, schemaName, tableName)).toUpdateCount)
    }
  }

  /**
    * Database Table Field-specific API routes (e.g. "/d/portfolio/stocks/amex/187/0")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @param rowID        the desired row by ID
    * @param columnID     the desired column by index
    * @return the [[Route]]
    */
  def routesByDatabaseTableColumnID(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    delete {
      // delete a field (e.g. "DELETE /d/portfolio/stocks/amex/287/0")
      complete(cpu.deleteField(ref, rowID, columnID).toUpdateCount(1))
    } ~
      get {
        // retrieve a field (e.g. "GET /d/portfolio/stocks/amex/287/0" ~> "CAKE")
        parameters('__contentType.?) { contentType_? =>
          val columns = cpu.getColumns(ref)
          val column = columns(columnID)
          val contentType = contentType_?.map(toContentType).getOrElse(toContentType(column.metadata.`type`))
          val field = cpu.getField(ref, rowID, columnID)
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
        // updates a field (e.g. "PUT /d/portfolio/stocks/amex/287/3" <~ 124.56)
        entity(as[String]) { value =>
          val columns = cpu.getColumns(ref)
          assert(columns.indices isDefinedAt columnID, throw ColumnOutOfRangeException(columnID))
          val columnType = columns(columnID).metadata.`type`
          complete(cpu.updateField(ref, rowID, columnID, Option(Codec.convertTo(value, columnType))).toUpdateCount(1))
        }
      }
  }

  /**
    * Database Table Range-specific API routes (e.g. "/r/portfolio/stocks/187/23")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @param start        the start of the range
    * @param length       the number of rows referenced
    * @return the [[Route]]
    */
  def routesByDatabaseTableRange(databaseName: String, schemaName: String, tableName: String, start: ROWID, length: Int): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    delete {
      // delete the range of rows (e.g. "DELETE /r/portfolio/stocks/amex/287/20")
      complete(cpu.deleteRange(ref, start, length).toUpdateCount)
    } ~
      get {
        // retrieve the range of rows (e.g. "GET /r/portfolio/stocks/amex/287/20")
        complete(cpu.getRange(ref, start, length).toList.map(_.toKeyValues))
      }
  }

  /**
    * Database Table Length-specific API routes (e.g. "/portfolio/stocks/187")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @param rowID        the referenced row ID
    * @return the [[Route]]
    */
  def routesByDatabaseTableRowID(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    delete {
      // delete the row by ID (e.g. "DELETE /d/portfolio/stocks/129")
      complete(cpu.deleteRow(ref, rowID).toUpdateCount(1))
    } ~
      get {
        // retrieve the row by ID (e.g. "GET /d/portfolio/stocks/287")
        complete(cpu.getRow(ref, rowID) match {
          case Some(row) => row.toKeyValues.toJson
          case None => JsObject()
        })
      } ~
      post {
        // partially update the row by ID
        // (e.g. "POST /d/portfolio/stocks/287" <~ { "lastSale":2.23, "lastSaleTime":1596404391000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.updateRow(ref, rowID, toExpressions(jsObject)).toUpdateCount(1))
        }
      } ~
      put {
        // replace the row by ID
        // (e.g. "PUT /d/portfolio/stocks/287" <~ { "exchange":"OTCBB", "symbol":"EVRX", "lastSale":2.09, "lastSaleTime":1596403991000 })
        entity(as[JsObject]) { jsObject =>
          complete(cpu.replaceRow(ref, rowID, toValues(jsObject)).toUpdateCount(1))
        }
      }
  }

  /**
    * Messaging-specific API routes (e.g. "/m/portfolio/stocks")
    * @param databaseName the name of the database
    * @param schemaName   the name of the schema
    * @param tableName    the name of the table
    * @return the [[Route]]
    */
  def routesMessaging(databaseName: String, schemaName: String, tableName: String): Route = {
    val ref = new EntityRef(databaseName, schemaName, tableName)
    post {
      // append the new message to the table
      // (e.g. "POST /m/portfolio/stocks/amex" <~ { "exchange":"OTCBB", "symbol":"EVRU", "lastSale":2.09, "lastSaleTime":1596403991000 })
      entity(as[JsObject]) { jsObject =>
        complete(cpu.insertRow(ref, toExpressions(jsObject)).toUpdateCount(1))
      }
    }
  }

  /**
    * Column search API routes (e.g. "/columns?database=shocktrade&table=stock%&column=symbol")
    * @return the [[Route]]
    */
  def routesSearchColumns: Route = {
    get {
      // search for columns (e.g. "GET /columns?database=shocktrade&table=stock%&column=symbol")
      extract(_.request.uri.query()) { params =>
        val (databasePattern_?, schemaPattern_?, tablePattern_?, columnPattern_?) = (params.get("database"), params.get("schema"), params.get("table"), params.get("column"))
        val results = searchColumns(databasePattern_?, schemaPattern_?, tablePattern_?, columnPattern_?)
        show("SearchColumns", results)("database" -> databasePattern_?, "schema" -> schemaPattern_?, "table" -> tablePattern_?, "column" -> columnPattern_?)
        complete(results)
      }
    }
  }

  /**
    * Database search API routes (e.g. "/databases?database=test%")
    * @return the [[Route]]
    */
  def routesSearchDatabases: Route = {
    get {
      // search for databases (e.g. "GET /databases?database=test%")
      extract(_.request.uri.query()) { params =>
        val databasePattern_? = params.get("database")
        val results = searchDatabases(databasePattern_?)
        show("SearchDatabases", results)("database" -> databasePattern_?)
        complete(results)
      }
    }
  }

  def routesSearchSchemas: Route = {
    get {
      // search for tables (e.g. "GET /schemas?database=test%&schema=stock%"")
      extract(_.request.uri.query()) { params =>
        val (databasePattern_?, schemaPattern_?) = (params.get("database"), params.get("schema"))
        val results = searchSchemas(databasePattern_?, schemaPattern_?)
        show("SearchSchemas", results)("database" -> databasePattern_?, "schema" -> schemaPattern_?)
        complete(results)
      }
    }
  }

  /**
    * Table search API routes (e.g. "/tables?database=test%&schema=public&table=stock%")
    * @return the [[Route]]
    */
  def routesSearchTables: Route = {
    get {
      // search for tables (e.g. "GET /tables?database=test%&table=stock%"")
      extract(_.request.uri.query()) { params =>
        val (databasePattern_?, schemaPattern_?, tablePattern_?) = (params.get("database"), params.get("schema"), params.get("table"))
        val results = searchTables(databasePattern_?, schemaPattern_?, tablePattern_?)
        show("SearchTables", results)("database" -> databasePattern_?, "schema" -> schemaPattern_?, "table" -> tablePattern_?)
        complete(results)
      }
    }
  }

  private def show[A](label: String, results: Seq[A])(params: (String, Option[String])*)(implicit w: JsonWriter[A]): Unit = {
    val pid = pidGenerator.addAndGet(1)
    logger.info(f"[$pid%05d] $label - URI: ?${params.map { case (k, v) => s"$k=${v.getOrElse("")}" }.mkString("&")}")
    results.zipWithIndex.foreach { case (r, n) => logger.info(f"${n + 1}%02d ${r.toJson}") }
    logger.info("")
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
      def asResult(databaseName: String, schemaName: String, tableName: String, limit: Option[Int]): QueryResult = {
        val ref = new EntityRef(databaseName, schemaName, tableName)
        response match {
          case Left(device) =>
            var rows: List[Seq[Option[Any]]] = Nil
            device.whileRow(KeyValues(), limit ?? Some(20)) { row => rows = row.fields.map(_.value) :: rows }
            QueryResult(ref, columns = device.columns, rows = rows)
          case Right(count) =>
            QueryResult(ref, columns = Nil, count = count)
        }
      }

      @inline
      def asUpdate: UpdateCount = response match {
        case Left(device) => UpdateCount(device.length)
        case Right(count) => UpdateCount(count)
      }
    }

    /**
      * Rich Table Summary
      * @param ts the host [[TableSummary table summary]]
      */
    final implicit class RichTableSummary(val ts: TableSummary) extends AnyVal {

      @inline
      def toURL(databaseName: String, host: String, port: Int): String = {
        s"http://$host:$port/d/$databaseName/${ts.schemaName}/${ts.tableName}"
      }
    }

  }

}
