package com.qwery.database.models

import com.qwery.models.TypeAsEnum

case class DatabaseConfig(types: Seq[TypeAsEnum])

object DatabaseConfig {
  import com.qwery.database.models.ModelsJsonProtocol._
  import spray.json._

  implicit val databaseConfigJsonFormat: RootJsonFormat[DatabaseConfig] = jsonFormat1(DatabaseConfig.apply)

}