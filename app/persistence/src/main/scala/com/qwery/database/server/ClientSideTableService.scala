package com.qwery.database
package server

import com.qwery.database.JSONSupport._
import com.qwery.database.models._
import com.qwery.database.server.DatabaseServerJsonProtocol._
import com.qwery.database.server.QxWebServiceClient._
import net.liftweb.json._
import spray.json._

/**
 * Client-Side Table Service
 * @param host the remote hostname
 * @param port the remote port
 */
case class ClientSideTableService(host: String = "0.0.0.0", port: Int) {
  private implicit val formats: DefaultFormats = DefaultFormats
  private val $http = new QxWebServiceClient()

  def createTable(databaseName: String, ref: TableCreation): UpdateCount = {
    $http.post(toUrl(databaseName), ref.toJSON.getBytes("utf-8")).as[UpdateCount]
  }

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): UpdateCount = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID").as[UpdateCount]
  }

  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateCount = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$start/$length").as[UpdateCount]
  }

  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateCount = {
    $http.delete(toUrl(databaseName, tableName, rowID)).as[UpdateCount]
  }

  def dropTable(databaseName: String, tableName: String): UpdateCount = {
    $http.delete(toUrl(databaseName, tableName)).as[UpdateCount]
  }

  def executeQuery(databaseName: String, sql: String): QueryResult = {
    $http.post(toQueryUrl(databaseName), body = sql.getBytes("utf-8")).toSprayJs.convertTo[QueryResult]
  }

  def findRows(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int] = None): Seq[KeyValues] = {
    $http.get(toUrl(databaseName, tableName, condition, limit)) match {
      case js: JArray => js.values.map(m => KeyValues(m.asInstanceOf[Map[String, Any]]))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getDatabaseMetrics(databaseName: String): DatabaseMetrics = {
    $http.get(toUrl(databaseName)).as[DatabaseMetrics]
  }

  def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte] = {
    $http.download(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID")
  }

  def getLength(databaseName: String, tableName: String): UpdateCount = {
    $http.get(url = s"${toUrl(databaseName, tableName)}/length").as[UpdateCount]
  }

  def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[KeyValues] = {
    $http.get(url = s"${toRangeUrl(databaseName, tableName)}/$start/$length") match {
      case js: JArray => js.values.map(m => KeyValues(m.asInstanceOf[Map[String, Any]]))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[KeyValues] = {
    $http.get(toUrl(databaseName, tableName, rowID)) match {
      case js: JObject => Option(KeyValues(js.values))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getRowWithMetadata(databaseName: String, tableName: String, rowID: ROWID): Row = {
    $http.get(toUrl(databaseName, tableName, rowID)).extract[Row]
  }

  def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    $http.get(toUrl(databaseName, tableName)).as[TableMetrics]
  }

  def insertRow(databaseName: String, tableName: String, values: KeyValues): UpdateCount = {
    $http.post(toUrl(databaseName, tableName), values.toJson.toString().getBytes("utf-8")).as[UpdateCount]
  }

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): UpdateCount = {
    $http.put(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes("utf-8"))
    UpdateCount(count = 1, __id = Some(rowID))
  }

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): UpdateCount = {
    $http.put(toUrl(databaseName, tableName, rowID, columnID), value.toJson.toString().getBytes("utf-8"))
    UpdateCount(count = 1, __id = Some(rowID))
  }

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: KeyValues): UpdateCount = {
    $http.post(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes("utf-8")).as[UpdateCount]
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

  private def toQueryUrl(databaseName: String): String = s"http://$host:$port/q/$databaseName"

  private def toRangeUrl(databaseName: String, tableName: String): String = s"http://$host:$port/r/$databaseName/$tableName"

  private def toUrl(databaseName: String): String = s"http://$host:$port/d/$databaseName"

  private def toUrl(databaseName: String, tableName: String): String = s"http://$host:$port/d/$databaseName/$tableName"

  private def toUrl(databaseName: String, tableName: String, condition: KeyValues, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(databaseName, tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = s"${toUrl(databaseName, tableName)}/$rowID"

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): String = s"${toUrl(databaseName, tableName)}/$rowID/$columnID"

}