package com.qwery.database.files

import com.qwery.database.JSONSupport._
import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, SHORT_BYTES}

case class TableColumn(name: String,
                       columnType: String,
                       sizeInBytes: Int,
                       comment: Option[String] = None,
                       enumValues: Seq[String] = Nil,
                       isCompressed: Boolean = false,
                       isEncrypted: Boolean = false,
                       isNullable: Boolean = true,
                       isPrimary: Boolean = false,
                       isRowID: Boolean = false) {

  /**
    * @return true, if the column is an enumeration type
    */
  def isEnum: Boolean = enumValues.nonEmpty

  override def toString: String = this.toJSON
}

object TableColumn {

  final implicit class ColumnToTableColumnConversion(val column: Column) extends AnyVal {
    @inline
    def toTableColumn: TableColumn = TableColumn(
      name = column.name,
      columnType = column.metadata.`type`.toString,
      comment = if (column.comment.nonEmpty) Some(column.comment) else None,
      enumValues = column.enumValues,
      sizeInBytes = if (column.isEnum) SHORT_BYTES else column.sizeInBytes,
      isCompressed = column.metadata.isCompressed,
      isEncrypted = column.metadata.isEncrypted,
      isNullable = column.metadata.isNullable,
      isPrimary = column.metadata.isPrimary,
      isRowID = column.metadata.isRowID,
    )
  }

  final implicit class TableColumnToColumnConversion(val column: TableColumn) extends AnyVal {
    @inline
    def toColumn: Column = new Column(
      name = column.name,
      comment = column.comment.getOrElse(""),
      enumValues = column.enumValues,
      sizeInBytes = if (column.isEnum) SHORT_BYTES else column.sizeInBytes,
      metadata = ColumnMetadata(
        `type` = ColumnTypes.withName(column.columnType),
        isCompressed = column.isCompressed,
        isEncrypted = column.isEncrypted,
        isNullable = column.isNullable,
        isPrimary = column.isPrimary,
        isRowID = column.isRowID
      ))
  }

}
