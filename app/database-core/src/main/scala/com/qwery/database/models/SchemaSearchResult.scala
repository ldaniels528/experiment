package com.qwery.database.models

case class SchemaSearchResult(databaseName: String, schemaName: String)

object SchemaSearchResult {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val schemaSearchResultJsonFormat: RootJsonFormat[SchemaSearchResult] = jsonFormat2(SchemaSearchResult.apply)

}
