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

  override def appendRow(tableName: String, row: TupleSet): UpdateResult = {
    $http.post[UpdateResult](toUrl(tableName), row.toJson.toString().getBytes("utf-8"))
  }

  override def deleteRow(tableName: String, rowID: ROWID): UpdateResult = {
    $http.delete[UpdateResult](toUrl(tableName, rowID))
  }

  override def dropTable(tableName: String): UpdateResult = {
    $http.delete[UpdateResult](toUrl(tableName))
  }

  override def executeQuery(sql: String): Seq[TupleSet] = {
    $http.postJSON(url = s"$toUrl/sql", content = sql.getBytes("utf-8")) match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def findRows(tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[TupleSet] = {
    $http.getJSON(toUrl(tableName, condition, limit)) match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getRow(tableName: String, rowID: ROWID): Option[TupleSet] = {
    $http.getJSON(toUrl(tableName, rowID)) match {
      case js: JObject => Option(js.values)
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getRows(tableName: String, start: ROWID, length: ROWID): Seq[TupleSet] = {
    $http.getJSON(url = s"${toUrl(tableName)}/$start/$length") match {
      case js: JArray => js.values.map(_.asInstanceOf[TupleSet])
      case js => throw new IllegalArgumentException(s"Unexpected type returned $js")
    }
  }

  override def getStatistics(tableName: String): TableFile.TableStatistics = {
    $http.get[TableFile.TableStatistics](toUrl(tableName))
  }

  private def toUrl: String = s"http://$host:$port"

  private def toUrl(tableName: String): String = s"http://$host:$port/tables/$tableName"

  private def toUrl(tableName: String, condition: TupleSet, limit: Option[Int]): String = {
    val keyValues = limit.toList.map(n => s"__limit=$n") ::: condition.toList.map { case (k, v) => s"$k=$v" }
    s"${toUrl(tableName)}?${keyValues.mkString("&")}"
  }

  private def toUrl(tableName: String, rowID: ROWID): String = s"${toUrl(tableName)}/$rowID"

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
