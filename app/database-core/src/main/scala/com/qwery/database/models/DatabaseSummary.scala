package com.qwery.database.models

/**
  * Represents a Database Summary
  * @param databaseName the database name
  * @param tables the collection of [[TableSummary table summaries]]
  */
case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary])

/**
  * Database Summary Companion
  */
object DatabaseSummary {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val databaseSummaryJsonFormat: RootJsonFormat[DatabaseSummary] = jsonFormat2(DatabaseSummary.apply)

}
