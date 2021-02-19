package com.qwery.database.models

case class TableSummary(tableName: String,
                        tableType: String,
                        description: Option[String],
                        lastModifiedTime: String,
                        fileSize: Long,
                        href: Option[String] = None)

object TableSummary {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val tableSummaryJsonFormat: RootJsonFormat[TableSummary] = jsonFormat6(TableSummary.apply)

}
