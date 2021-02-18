package com.qwery.database.models

import com.qwery.database.Column

case class ColumnSearchResult(databaseName: String, tableName: String, column: Column)

object ColumnSearchResult {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val columnSearchResultJsonFormat: RootJsonFormat[ColumnSearchResult] = jsonFormat3(ColumnSearchResult.apply)

}
