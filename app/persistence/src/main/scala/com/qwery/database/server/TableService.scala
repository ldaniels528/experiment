package com.qwery.database.server

import com.qwery.database.server.JSONSupport.JSONProductConversion
import com.qwery.database.server.TableService.TableColumn.ColumnToTableColumnConversion
import com.qwery.database.server.TableService._
import com.qwery.database.{Column, ColumnMetadata, ColumnTypes, ROWID}

/**
 * Table Service
 */
trait TableService[R] {

  def appendRow(databaseName: String, tableName: String, values: TupleSet): QueryResult

  def createTable(databaseName: String, ref: TableCreation): QueryResult

  def deleteField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): QueryResult

  def deleteRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): QueryResult

  def deleteRow(databaseName: String, tableName: String, rowID: ROWID): QueryResult

  def dropTable(databaseName: String, tableName: String): QueryResult

  def executeQuery(databaseName: String, sql: String): QueryResult

  def findRows(databaseName: String, tableName: String, condition: TupleSet, limit: Option[Int] = None): Seq[R]

  def getDatabaseMetrics(databaseName: String): DatabaseMetrics

  def getField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte]

  def getLength(databaseName: String, tableName: String): QueryResult

  def getRange(databaseName: String, tableName: String, start: ROWID, length: ROWID): Seq[R]

  def getRow(databaseName: String, tableName: String, rowID: ROWID): Option[R]

  def getTableMetrics(databaseName: String, tableName: String): TableMetrics

  def replaceRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult

  def updateField(databaseName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): QueryResult

  def updateRow(databaseName: String, tableName: String, rowID: ROWID, values: TupleSet): QueryResult

}

/**
 * Table Service Companion
 */
object TableService {

  case class DatabaseMetrics(databaseName: String,
                             tables: Seq[String],
                             responseTimeMillis: Double = 0) {
    override def toString: String = this.toJSON
  }

  case class LoadMetrics(records: Long, ingestTime: Double, recordsPerSec: Double) {
    override def toString: String = this.toJSON
  }

  case class QueryResult(databaseName: String,
                         tableName: String,
                         responseTime: Double,
                         columns: Seq[TableColumn] = Nil,
                         rows: Seq[Seq[Option[Any]]] = Nil,
                         count: Int = 0,
                         __id: Option[Int] = None,
                         __ids: List[Int] = Nil) {
    override def toString: String = this.toJSON
  }

  case class TableColumn(name: String,
                         columnType: String,
                         sizeInBytes: Int,
                         comment: Option[String] = None,
                         isCompressed: Boolean = false,
                         isEncrypted: Boolean = false,
                         isNullable: Boolean = true,
                         isPrimary: Boolean = false,
                         isRowID: Boolean = false) {
    override def toString: String = this.toJSON
  }

  object TableColumn {

    final implicit class ColumnToTableColumnConversion(val column: Column) extends AnyVal {
      @inline
      def toTableColumn: TableColumn = TableColumn(
        name = column.name,
        columnType = column.metadata.`type`.toString,
        comment = if (column.comment.nonEmpty) Some(column.comment) else None,
        sizeInBytes = column.sizeInBytes,
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
        sizeInBytes = column.sizeInBytes,
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

  case class TableConfig(columns: Seq[TableColumn], indices: Seq[TableIndexRef]){
    override def toString: String = this.toJSON
  }

  case class TableIndexRef(indexName: String, indexColumn: String){
    override def toString: String = this.toJSON
  }

  case class TableMetrics(databaseName: String,
                          tableName: String,
                          columns: Seq[TableColumn],
                          physicalSize: Option[Long],
                          recordSize: Int,
                          rows: ROWID,
                          responseTimeMillis: Double = 0){
    override def toString: String = this.toJSON
  }

}