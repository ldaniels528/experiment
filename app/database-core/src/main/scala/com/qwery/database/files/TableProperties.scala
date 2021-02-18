package com.qwery.database.files

import com.qwery.database.Column

/**
  * Represents a table's properties
  * @param description  the table description
  * @param columns      the table columns
  * @param isColumnar   indicates whether the table is column-based
  * @param ifNotExists  if false, an error when the table already exists
  */
case class TableProperties(description: Option[String], columns: Seq[Column], isColumnar: Boolean, ifNotExists: Boolean)

/**
  * TableProperties Companion
  */
object TableProperties {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._
  implicit val tablePropertiesJsonFormat: RootJsonFormat[TableProperties] = jsonFormat4(TableProperties.apply)

  /**
    * Creates a new table properties
    * @param description  the table description
    * @param columns      the table columns
    * @param isColumnar   indicates whether the table is column-based
    * @param ifNotExists  if false, an error when the table already exists
    * @return [[TableProperties]]
    */
  def create(description: Option[String], columns: Seq[Column], isColumnar: Boolean = false, ifNotExists: Boolean = false): TableProperties = {
    new TableProperties(description, columns, isColumnar, ifNotExists)
  }

}