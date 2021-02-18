package com.qwery.database.device

/**
  * Represents a reference to a Table Index
  * @param databaseName    the name of the database
  * @param tableName       the name of the host table
  * @param indexColumnName the name of the index column
  */
case class TableIndexRef(databaseName: String, tableName: String, indexColumnName: String)

/**
 * Table Index Reference Companion
 */
object TableIndexRef {
  import spray.json._
  import DefaultJsonProtocol._
  implicit val tableIndexJsonFormat: RootJsonFormat[TableIndexRef] = jsonFormat3(TableIndexRef.apply)

}