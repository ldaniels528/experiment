package com.qwery.database.models

import com.qwery.database.models.TableConfig.{ExternalTableConfig, PhysicalTableConfig, VirtualTableConfig}
import com.qwery.models.TableIndex

/**
  * Represents a Table Configuration
  * @param columns       the table [[Column columns]]
  * @param description   the optional table description
  * @param externalTable the optional [[ExternalTableConfig external table configuration]]
  * @param indices       the optional collection of [[TableIndex index references]]
  * @param physicalTable the optional [[PhysicalTableConfig physical table configuration]]
  * @param virtualTable  the optional [[VirtualTableConfig virtual table configuration]]
  */
case class TableConfig(columns: Seq[Column],
                       description: Option[String] = None,
                       externalTable: Option[ExternalTableConfig] = None,
                       indices: Seq[TableIndex] = Nil,
                       physicalTable: Option[PhysicalTableConfig] = None,
                       virtualTable: Option[VirtualTableConfig] = None)

/**
  * Table Config Companion
  */
object TableConfig {

  case class ExternalTableConfig(format: Option[String],
                                 location: Option[String],
                                 fieldTerminator: Option[String] = None,
                                 lineTerminator: Option[String] = None,
                                 headersIncluded: Option[Boolean] = None,
                                 nullValue: Option[String] = None)

  case class PhysicalTableConfig(location: String)

  case class VirtualTableConfig(queryString: String)

}