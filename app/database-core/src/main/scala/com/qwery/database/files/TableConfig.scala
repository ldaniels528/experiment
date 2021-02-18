package com.qwery.database.files

import com.qwery.database.Column
import com.qwery.database.device.TableIndexRef
import com.qwery.database.files.TableConfig.ExternalTableConfig

/**
  * Represents a Table Config
  * @param columns       the table [[Column column]]
  * @param isColumnar    indicates whether the table is columnar
  * @param indices       the collection of [[TableIndexRef index references]]
  * @param description   the table description
  * @param externalTable the [[ExternalTableConfig external table configuration]]
  */
case class TableConfig(columns: Seq[Column],
                       isColumnar: Boolean = false,
                       indices: Seq[TableIndexRef] = Nil,
                       description: Option[String] = None,
                       externalTable: Option[ExternalTableConfig] = None)

/**
  * Table Config Companion
  */
object TableConfig {
  import spray.json._
  import DefaultJsonProtocol._

  implicit val extTableRefJsonFormat: RootJsonFormat[ExternalTableConfig] = jsonFormat6(ExternalTableConfig.apply)

  implicit val tableConfigJsonFormat: RootJsonFormat[TableConfig] = jsonFormat5(TableConfig.apply)

  case class ExternalTableConfig(format: Option[String],
                                 location: Option[String],
                                 fieldTerminator: Option[String] = None,
                                 lineTerminator: Option[String] = None,
                                 headersIncluded: Option[Boolean] = None,
                                 nullValue: Option[String] = None)

}