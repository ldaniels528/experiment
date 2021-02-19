package com.qwery.database.models

case class DatabaseSearchResult(databaseName: String)

object DatabaseSearchResult {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val databaseSearchResultJsonFormat: RootJsonFormat[DatabaseSearchResult] = jsonFormat1(DatabaseSearchResult.apply)

}
