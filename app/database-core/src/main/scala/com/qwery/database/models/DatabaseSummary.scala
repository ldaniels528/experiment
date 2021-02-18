package com.qwery.database.models

case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary])

object DatabaseSummary {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val databaseSummaryJsonFormat: RootJsonFormat[DatabaseSummary] = jsonFormat2(DatabaseSummary.apply)

}
