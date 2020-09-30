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

  override def appendRow(databaseName: String, tableName: String, row: TupleSet): UpdateResult = {
    $http.post[UpdateResult](toUrl(databaseName, tableName), row.toJson.toString().getBytes("utf-8"))
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
    $http.postJSON(url = s"${toUrl(databaseName)}/sql", content = sql.getBytes("utf-8")) match {
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

  override def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[TupleSet] = {
    $http.getJSON(toUrl(databaseName, tableName, rowID)) match {
      case js: JObject => Option(js.values)
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    $http.getJSON(url = s"${toUrl(databaseName, tableName)}/$start/$length") match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getTableMetrics(databaseName: String, tableName: String): TableFile.TableMetrics = {
    $http.get[TableFile.TableMetrics](toUrl(databaseName, tableName))
  }

  private def toUrl(databaseName: String): String = s"http://$host:$port/$databaseName"

  private def toUrl(databaseName: String, tableName: String): String = s"http://$host:$port/$databaseName/$tableName"

  private def toUrl(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(databaseName, tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = s"${toUrl(databaseName, tableName)}/$rowID"

  private def unwrapJSON(jValue: JValue): Any = jValue match {
    case JArray(array) => array.map(unwrapJSON)
    case JBool(value) => value
    case JDouble(value) => value
    case JInt(value) => value
    case JNull | JNothing => null
    case js: JObject => js.values
    case JString(value) => value
    case x => throw new IllegalArgumentException(s"Unsupported type $x (${x.getClass.getName})")
  }

}
