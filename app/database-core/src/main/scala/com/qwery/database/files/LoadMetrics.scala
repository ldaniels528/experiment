package com.qwery.database.files

case class LoadMetrics(records: Long, ingestTime: Double, recordsPerSec: Double)

object LoadMetrics {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._

  implicit val loadMetricsJsonFormat: RootJsonFormat[LoadMetrics] = jsonFormat3(LoadMetrics.apply)

}