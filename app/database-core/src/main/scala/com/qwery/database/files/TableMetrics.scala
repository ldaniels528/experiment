package com.qwery.database.files

import com.qwery.database.{Column, ROWID}

case class TableMetrics(databaseName: String,
                        tableName: String,
                        columns: Seq[Column],
                        physicalSize: Option[Long],
                        recordSize: Int,
                        rows: ROWID)

object TableMetrics {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val tableMetricsJsonFormat: RootJsonFormat[TableMetrics] = jsonFormat6(TableMetrics.apply)

}