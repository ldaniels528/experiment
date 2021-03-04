package com.qwery.database.models

import com.qwery.database.models.TableConfig.ExternalTableConfig
import com.qwery.models.TableIndex

/**
  * Represents a Table Config
  * @param columns       the table [[Column column]]
  * @param indices       the collection of [[TableIndex index references]]
  * @param description   the table description
  * @param externalTable the [[ExternalTableConfig external table configuration]]
  */
case class TableConfig(columns: Seq[Column],
                       indices: Seq[TableIndex] = Nil,
                       description: Option[String] = None,
                       externalTable: Option[ExternalTableConfig] = None)

/**
  * Table Config Companion
  */
object TableConfig {
  import ModelsJsonProtocol._
  import spray.json._

  implicit val extTableRefJsonFormat: RootJsonFormat[ExternalTableConfig] = jsonFormat6(ExternalTableConfig.apply)

  implicit val tableConfigJsonFormat: RootJsonFormat[TableConfig] = jsonFormat4(TableConfig.apply)

  case class ExternalTableConfig(format: Option[String],
                                 location: Option[String],
                                 fieldTerminator: Option[String] = None,
                                 lineTerminator: Option[String] = None,
                                 headersIncluded: Option[Boolean] = None,
                                 nullValue: Option[String] = None)

}