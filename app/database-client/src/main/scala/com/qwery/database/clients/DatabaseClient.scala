package com.qwery.database
package clients

import com.qwery.database.JSONSupport._
import com.qwery.database.WebServiceClient._
import com.qwery.database.files.{TableMetrics, TableProperties}
import com.qwery.database.models.DatabaseJsonProtocol._
import com.qwery.database.models._
import spray.json._

import java.io.File
import java.net.URLEncoder

/**
 * Qwery Database Client
 * @param host the remote hostname
 * @param port the remote port
 */
case class DatabaseClient(host: String = "0.0.0.0", port: Int) {
  private val charSetName = "utf-8"
  private val $http = new WebServiceClient()
  private var closed = false

  /**
    * Closes the connection to the server
    */
  def close(): Unit = closed = true

  /**
    * Indicates whether the connection to the server has been closed
    * @return true, if [[close()]] has previously been called
    */
  def isClosed: Boolean = closed

  /**
    * Creates a new table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param properties   the [[TableProperties table properties]]
    * @return the [[UpdateCount update count]]
    */
  def createTable(databaseName: String, tableName: String, properties: TableProperties): UpdateCount = {
    $http.put(toUrl(databaseName, tableName), properties.toJSON.getBytes(charSetName))
    UpdateCount(count = 1, __id = None)
  }

  /**
    * Deletes the contents of a field; rending it null.
    * @param databaseName the database name
    * @param tableName    the table name
    * @param rowID        the row ID of the field
    * @param columnID     the column ID of the field
    * @return the [[UpdateCount update count]]
    */
  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): UpdateCount = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID").as[UpdateCount]
  }

  /**
    * Deletes a range of rows in the database
    * @param databaseName the database name
    * @param tableName    the table name
    * @param start        the initial row ID
    * @param length       the number of rows to delete
    * @return the [[UpdateCount update count]]
    */
  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateCount = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$start/$length").as[UpdateCount]
  }

  /**
    * Deletes a row by ID
    * @param databaseName the database name
    * @param tableName    the table name
    * @param rowID        the ID of the row to delete
    * @return the [[UpdateCount update count]]
    */
  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateCount = {
    $http.delete(toUrl(databaseName, tableName, rowID)).as[UpdateCount]
  }

  /**
    * Drops a database table
    * @param databaseName the database name
    * @param tableName    the table name
    * @param ifExists     indicates whether an existence check should be performed
    * @return the [[UpdateCount update count]]
    */
  def dropTable(databaseName: String, tableName: String, ifExists: Boolean): UpdateCount = {
    $http.delete(toUrl(databaseName, tableName)).as[UpdateCount]
  }

  /**
    * Executes a SQL statement or query
    * @param databaseName the database name
    * @param sql          the SQL statement or query
    * @return the [[QueryResult]]
    */
  def executeQuery(databaseName: String, sql: String): QueryResult = {
    $http.post(toQueryUrl(databaseName), body = sql.getBytes(charSetName)).convertTo[QueryResult]
  }

  /**
    * Retrieves rows matching the given criteria (up to the optionally specified limit)
    * @param databaseName the database name
    * @param tableName    the table name
    * @param condition    the deletion criteria
    * @param limit        the maximum number of records to delete
    * @return the the updated [[KeyValues row]]
    */
  def findRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int] = None): Seq[KeyValues] = {
    $http.get(toUrl(databaseName, tableName, condition, limit)) match {
      case js: JsArray => js.elements.map(unwrap).map(m => KeyValues(m.asInstanceOf[Map[String, Any]]))
      case js => die(s"Unexpected type returned $js")
    }
  }

  /**
    * Retrieves the metrics for the specified database
    * @param databaseName the specified database
    * @return the [[DatabaseSummary]]
    */
  def getDatabaseSummary(databaseName: String): DatabaseSummary = {
    $http.get(toUrl(databaseName)).as[DatabaseSummary]
  }

  def getFieldAsBytes(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte] = {
    $http.getAsBytes(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID")
  }

  def getFieldAsFile(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): File = {
    $http.getAsFile(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID")
  }

  def getLength(databaseName: String, tableName: String): UpdateCount = {
    $http.get(url = s"${toUrl(databaseName, tableName)}/length").as[UpdateCount]
  }

  /**
    * Retrieves a range of records
    * @param databaseName the database name
    * @param tableName    the table name
    * @param start        the beginning of the range
    * @param length       the number of records to retrieve
    * @return the collection of [[Row rows]]
    */
  def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[KeyValues] = {
    $http.get(url = s"${toRangeUrl(databaseName, tableName)}/$start/$length") match {
      case js: JsArray => js.elements.map(unwrap).map(m => KeyValues(m.asInstanceOf[Map[String, Any]]))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[KeyValues] = {
    $http.get(toUrl(databaseName, tableName, rowID)) match {
      case js: JsObject => Option(KeyValues(js.fields.map { case (name, jv) => name -> unwrap(jv)}))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getRowWithMetadata(databaseName: String, tableName: String, rowID: ROWID): Row = {
    $http.get(toUrl(databaseName, tableName, rowID)).convertTo[Row]
  }

  def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    $http.get(toUrl(databaseName, tableName)).as[TableMetrics]
  }

  def insertRow(databaseName: String, tableName: String, values: KeyValues): UpdateCount = {
    $http.post(toUrl(databaseName, tableName), values.toJson.toString().getBytes(charSetName)).as[UpdateCount]
  }

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): UpdateCount = {
    $http.put(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes(charSetName))
    UpdateCount(count = 1, __id = Some(rowID))
  }

  def searchColumns(databaseNamePattern: Option[String] = None, tableNamePattern: Option[String] = None, columnNamePattern: Option[String] = None): List[ColumnSearchResult] = {
    $http.get(toSearchUrl(category = "columns", databaseNamePattern, tableNamePattern, columnNamePattern)).as[List[ColumnSearchResult]]
  }

  def searchDatabases(databaseNamePattern: Option[String] = None): List[DatabaseSearchResult] = {
    $http.get(toSearchUrl(category = "databases", databaseNamePattern)).as[List[DatabaseSearchResult]]
  }

  def searchTables(databaseNamePattern: Option[String] = None, tableNamePattern: Option[String] = None): List[TableSearchResult] = {
    $http.get(toSearchUrl(category = "tables", databaseNamePattern, tableNamePattern)).as[List[TableSearchResult]]
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): UpdateCount = {
    $http.put(toUrl(databaseName, tableName, rowID, columnID), value.toJson.toString().getBytes(charSetName))
    UpdateCount(count = 1, __id = Some(rowID))
  }

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): UpdateCount = {
    $http.post(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes(charSetName)).as[UpdateCount]
  }

  def toIterator(databaseName: String, tableName: String): Iterator[KeyValues] = new Iterator[KeyValues] {
    private var rowID: ROWID = 0
    private val eof: ROWID = getLength(databaseName, tableName).__id.getOrElse(0: ROWID)

    def hasNext: Boolean = rowID < eof

    def next(): KeyValues = {
      if (!hasNext) throw new IndexOutOfBoundsException()
      val row = getRow(databaseName, tableName, rowID)
      rowID += 1
      row.orNull
    }
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toQueryUrl(databaseName: String): String = s"http://$host:$port/q/$databaseName"

  private def toRangeUrl(databaseName: String, tableName: String): String = s"http://$host:$port/r/$databaseName/$tableName"

  private def toSearchUrl(category: String, databasePattern: Option[String] = None, tablePattern: Option[String] = None, columnPattern: Option[String] = None): String = {
    val queryString = Seq("database" -> databasePattern, "table" -> tablePattern, "column" -> columnPattern) flatMap {
      case (name, value_?) => value_?.map(value => s"$name=${URLEncoder.encode(value, charSetName)}")
    } mkString "&"
    s"http://$host:$port/$category?$queryString"
  }

  private def toUrl(databaseName: String): String = s"http://$host:$port/d/$databaseName"

  private def toUrl(databaseName: String, tableName: String): String = s"http://$host:$port/d/$databaseName/$tableName"

  private def toUrl(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(databaseName, tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = s"${toUrl(databaseName, tableName)}/$rowID"

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): String = s"${toUrl(databaseName, tableName)}/$rowID/$columnID"

}