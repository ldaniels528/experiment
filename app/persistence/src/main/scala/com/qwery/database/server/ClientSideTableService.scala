package com.qwery.database.server

import com.qwery.database.ROWID
import com.qwery.database.server.QweryCustomJsonProtocol._
import com.qwery.database.server.TableService.UpdateResult
import net.liftweb.json._
import spray.json._

/**
 * Client-Side Table Service
 */
case class ClientSideTableService(host: String = "0.0.0.0", port: Int) extends TableService[TupleSet] {
  private val $http = new QweryHttpClient()

  override def appendRow(databaseName: String, tableName: String, values: TupleSet): UpdateResult = {
    $http.post[UpdateResult](toUrl(databaseName, tableName), values.toJson.toString().getBytes("utf-8"))
  }

  override def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): UpdateResult = {
    $http.delete[UpdateResult](url = s"${toUrl(databaseName, tableName)}/$start/$length")
  }

  override def deleteRow(databaseName: String, tableName: String, rowID: ROWID): UpdateResult = {
    $http.delete[UpdateResult](toUrl(databaseName, tableName, rowID))
  }

  override def dropTable(databaseName: String, tableName: String): UpdateResult = {
    $http.delete[UpdateResult](toUrl(databaseName, tableName))
  }

  override def executeQuery(databaseName: String, sql: String): Seq[TupleSet] = {
    $http.postJSON(toUrl(databaseName), content = sql.getBytes("utf-8")) match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[TupleSet] = {
    $http.getJSON(toUrl(databaseName, tableName, condition, limit)) match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getDatabaseMetrics(databaseName: String): TableFile.DatabaseMetrics = {
    $http.get[TableFile.DatabaseMetrics](toUrl(databaseName))
  }

  override def getLength(databaseName: String, tableName: String): UpdateResult = {
    $http.get[UpdateResult](url = s"${toUrl(databaseName, tableName)}/length")
  }

  override def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    $http.getJSON(url = s"${toUrl(databaseName, tableName)}/$start/$length") match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[TupleSet] = {
    $http.getJSON(toUrl(databaseName, tableName, rowID)) match {
      case js: JObject => Option(js.values)
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getTableMetrics(databaseName: String, tableName: String): TableFile.TableMetrics = {
    $http.get[TableFile.TableMetrics](toUrl(databaseName, tableName))
  }

  override def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): UpdateResult = {
    val (_, responseTime) = time($http.put(toUrl(databaseName, tableName, rowID), values.toJson.toString().getBytes("utf-8")))
    UpdateResult(count = 1, responseTime, __id = Some(rowID))
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

  private def toUrl(databaseName: String): String = s"http://$host:$port/$databaseName"

  private def toUrl(databaseName: String, tableName: String): String = s"http://$host:$port/$databaseName/$tableName"

  private def toUrl(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(databaseName, tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = s"${toUrl(databaseName, tableName)}/$rowID"

}
