package com.qwery.database

import com.qwery.database.models.TableColumn.ColumnToTableColumnConversion
import JSONSupport.JSONProductConversion
import com.qwery.models.TypeAsEnum

package object models {

  case class DatabaseConfig(types: Seq[TypeAsEnum]) {
    override def toString: String = this.toJSON
  }

  case class DatabaseMetrics(databaseName: String, tables: Seq[String]) {
    override def toString: String = this.toJSON
  }

  case class LoadMetrics(records: Long, ingestTime: Double, recordsPerSec: Double) {
    override def toString: String = this.toJSON
  }

  case class QueryResult(databaseName: String,
                         tableName: String,
                         columns: Seq[TableColumn] = Nil,
                         rows: Seq[Seq[Option[Any]]] = Nil,
                         count: Int = 0,
                         __ids: Seq[Int] = Nil) {
    override def toString: String = this.toJSON
  }

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

  case class TableCreation(tableName: String, columns: Seq[TableColumn])

  object TableCreation {
    def create(tableName: String, columns: Seq[Column]) = new TableCreation(tableName, columns.map(_.toTableColumn))
  }

  case class TableConfig(columns: Seq[TableColumn], indices: Seq[TableIndexRef]) {
    override def toString: String = this.toJSON
  }

  /**
   * Represents a reference to a Table Index
   * @param databaseName    the name of the database
   * @param tableName       the name of the host table
   * @param indexColumnName the name of the index column
   */
  case class TableIndexRef(databaseName: String, tableName: String, indexColumnName: String) {
    override def toString: String = this.toJSON
  }

  case class TableMetrics(databaseName: String,
                          tableName: String,
                          columns: Seq[TableColumn],
                          physicalSize: Option[Long],
                          recordSize: Int,
                          rows: ROWID) {
    override def toString: String = this.toJSON
  }

  case class UpdateCount(count: Int, __id: Option[Int] = None) {
    override def toString: String = this.toJSON
  }

}
