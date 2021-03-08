package com.qwery.database
package clients

import com.qwery.database.models.ModelsJsonProtocol._
import com.qwery.database.models.UpdateCount
import com.qwery.database.util.WebServiceClient
import com.qwery.database.util.WebServiceClient.QweryResponseConversion

/**
 * Qwery Message Producer
 * @param host the remote hostname
 * @param port the remote port
 */
case class MessageProducer(host: String = "0.0.0.0", port: Int) {
  private val $http = new WebServiceClient()
  private val charSetName = "utf-8"

  /**
   * Appends a new message to the table
   * @param databaseName the database name
   * @param tableName    the table name
   * @param message      the JSON message to append
   * @return the [[UpdateCount response]]
   */
  def send(databaseName: String, schemaName: String, tableName: String, message: String): UpdateCount = {
    $http.post(toUrl(databaseName, schemaName, tableName), body = message.getBytes(charSetName)).as[UpdateCount]
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toUrl(databaseName: String, schemaName: String, tableName: String): String = {
    s"http://$host:$port/m/$databaseName/$schemaName/$tableName"
  }

}