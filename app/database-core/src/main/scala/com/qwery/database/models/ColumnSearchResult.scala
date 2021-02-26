package com.qwery.database.models

case class ColumnSearchResult(databaseName: String, schemaName: String, tableName: String, column: Column)

object ColumnSearchResult {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val columnSearchResultJsonFormat: RootJsonFormat[ColumnSearchResult] = jsonFormat4(ColumnSearchResult.apply)

}
