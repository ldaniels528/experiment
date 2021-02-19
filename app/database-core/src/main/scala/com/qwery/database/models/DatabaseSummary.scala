package com.qwery.database.models

case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary])

object DatabaseSummary {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val databaseSummaryJsonFormat: RootJsonFormat[DatabaseSummary] = jsonFormat2(DatabaseSummary.apply)

}
