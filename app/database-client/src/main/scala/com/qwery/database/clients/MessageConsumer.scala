package com.qwery.database.clients

import com.qwery.database.{KeyValues, ROWID, WebServiceClient, die}
import net.liftweb.json.JObject

import scala.util.{Failure, Success, Try}

/**
 * Qwery Message Consumer
 * @param host         the remote hostname
 * @param port         the remote port
 * @param databaseName the database name
 * @param tableName    the table name
 */
case class MessageConsumer(host: String = "0.0.0.0", port: Int, databaseName: String, tableName: String) {
  private val $http = new WebServiceClient()
  private var rowID: ROWID = 0

  def getMessage(offset: ROWID): Option[KeyValues] = {
    $http.get(toUrl(databaseName, tableName, offset)) match {
      case js: JObject => Option(KeyValues(js.values))
      case js => die(s"Unexpected type returned $js")
    }
  }

  def getNextMessage: Option[KeyValues] = {
    Try(getMessage(rowID)) match {
      case Success(value) =>
        rowID += 1
        value
      case Failure(e) =>
        e.printStackTrace()
        None
    }
  }

  def seek(offset: ROWID): Unit = {
    assert(offset >= 0, "Negative offsets are not supported")
    rowID = offset
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toUrl(databaseName: String, tableName: String, rowID: ROWID): String = {
    s"http://$host:$port/m/$databaseName/$tableName/$rowID"
  }

}