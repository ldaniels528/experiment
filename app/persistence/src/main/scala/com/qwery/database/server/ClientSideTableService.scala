package com.qwery.database.server

import com.qwery.database.server.JSONSupport._
import com.qwery.database.server.QweryCustomJsonProtocol._
import com.qwery.database.server.QweryWebServiceClient._
import com.qwery.database.server.TableService._
import com.qwery.database.{ROWID, Row}
import net.liftweb.json._
import spray.json._

/**
 * Client-Side Table Service
 */
case class ClientSideTableService(host: String = "0.0.0.0", port: Int) extends TableService[TupleSet] {
  private implicit val formats: DefaultFormats = DefaultFormats
  private val $http = new QweryWebServiceClient()

  override def appendRow(databaseName: String, tableName: String, values: TupleSet): QueryResult = {
    $http.post(toUrl(databaseName, tableName), values.toJson.toString().getBytes("utf-8")).as[QueryResult]
  }

  override def createTable(databaseName: String, ref: TableCreation): QueryResult = {
    $http.post(toUrl(databaseName), ref.toJSON.getBytes("utf-8")).as[QueryResult]
  }

  override def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): QueryResult = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID").as[QueryResult]
  }

  override def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): QueryResult = {
    $http.delete(url = s"${toUrl(databaseName, tableName)}/$start/$length").as[QueryResult]
  }

  override def deleteRow(databaseName: String, tableName: String, rowID: ROWID): QueryResult = {
    $http.delete(toUrl(databaseName, tableName, rowID)).as[QueryResult]
  }

  override def dropTable(databaseName: String, tableName: String): QueryResult = {
    $http.delete(toUrl(databaseName, tableName)).as[QueryResult]
  }

  override def executeQuery(databaseName: String, sql: String): QueryResult = {
    $http.post(toQueryUrl(databaseName), body = sql.getBytes("utf-8")).toSprayJs.convertTo[QueryResult]
  }

  override def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[TupleSet] = {
    $http.get(toUrl(databaseName, tableName, condition, limit)) match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getDatabaseMetrics(databaseName: String): DatabaseMetrics = {
    $http.get(toUrl(databaseName)).as[DatabaseMetrics]
  }

  override def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte] = {
    $http.download(url = s"${toUrl(databaseName, tableName)}/$rowID/$columnID")
  }

  override def getLength(databaseName: String, tableName: String): QueryResult = {
    $http.get(url = s"${toUrl(databaseName, tableName)}/length").as[QueryResult]
  }

  override def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    $http.get(url = s"${toRangeUrl(databaseName, tableName)}/$start/$length") match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[TupleSet] = {
    $http.get(toUrl(databaseName, tableName, rowID)) match {
      case js: JObject => Option(js.values)
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  def getRowWithMetadata(databaseName: String, tableName: String, rowID: ROWID): Row = {
    $http.get(toUrl(databaseName, tableName, rowID)).extract[Row]
  }

  override def getTableMetrics(databaseName: String, tableName: String): TableMetrics = {
    $http.get(toUrl(databaseName, tableName)).as[TableMetrics]
  }

  override def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult = {
    val (_, responseTime) = time($http.put(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes("utf-8")))
    QueryResult(databaseName, tableName, count = 1, responseTime = responseTime)
  }

  override def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): QueryResult = {
    val (_, responseTime) = time($http.put(toUrl(databaseName, tableName, rowID, columnID), value.toJson.toString().getBytes("utf-8")))
    QueryResult(databaseName, tableName, count = 1, responseTime = responseTime)
  }

  override def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult = {
    $http.post(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes("utf-8")).as[QueryResult]
  }

  def toIterator(databaseName: String, tableName: String): Iterator[TupleSet] = new Iterator[TupleSet] {
    private var rowID: ROWID = 0
    private val eof: ROWID = getLength(databaseName, tableName).__id.getOrElse(0: ROWID)

    override def hasNext: Boolean = rowID < eof

    override def next(): TupleSet = {
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

  private def toUrl(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(databaseName, tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = s"${toUrl(databaseName, tableName)}/$rowID"

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): String = s"${toUrl(databaseName, tableName)}/$rowID/$columnID"

}
