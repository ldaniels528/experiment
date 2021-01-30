package com.qwery.database.files

import com.qwery.database.Column
import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.database.files.TableColumn.ColumnToTableColumnConversion

/**
  * Represents a table's properties
  * @param description  the table description
  * @param columns      the table columns
  * @param isColumnar   indicates whether the table is column-based
  * @param ifNotExists  if false, an error when the table already exists
  */
case class TableProperties(description: Option[String], columns: Seq[TableColumn], isColumnar: Boolean, ifNotExists: Boolean) {
  override def toString: String = this.toJSON
}

/**
  * TableProperties Companion
  */
object TableProperties {

  /**
    * Creates a new table properties
    * @param description  the table description
    * @param columns      the table columns
    * @param isColumnar   indicates whether the table is column-based
    * @param ifNotExists  if false, an error when the table already exists
    * @return [[TableProperties]]
    */
  def create(description: Option[String], columns: Seq[Column], isColumnar: Boolean = false, ifNotExists: Boolean = false): TableProperties = {
    new TableProperties(description, columns.map(_.toTableColumn), isColumnar, ifNotExists)
  }

}