package com.qwery.database.models

import com.qwery.database.ROWID
import com.qwery.models.EntityRef

case class TableMetrics(ref: EntityRef,
                        columns: Seq[Column],
                        physicalSize: Option[Long],
                        recordSize: Int,
                        rows: ROWID)

object TableMetrics {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val tableMetricsJsonFormat: RootJsonFormat[TableMetrics] = jsonFormat5(TableMetrics.apply)

}