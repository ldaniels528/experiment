package com.qwery.database.models

case class TableSearchResult(databaseName: String, tableName: String)

object TableSearchResult {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val tableSearchResultJsonFormat: RootJsonFormat[TableSearchResult] = jsonFormat2(TableSearchResult.apply)

}
