package com.qwery.database.models

import com.qwery.database.ROWID

case class TableMetrics(databaseName: String,
                        tableName: String,
                        columns: Seq[Column],
                        physicalSize: Option[Long],
                        recordSize: Int,
                        rows: ROWID)

object TableMetrics {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val tableMetricsJsonFormat: RootJsonFormat[TableMetrics] = jsonFormat6(TableMetrics.apply)

}