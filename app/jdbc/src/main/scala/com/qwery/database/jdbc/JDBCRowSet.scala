package com.qwery.database
package jdbc

import java.nio.ByteBuffer
import java.sql.RowId

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.jdbc.JDBCRowSet.uninited
import com.qwery.database.models.TableColumn
import org.slf4j.LoggerFactory

/**
  * Qwery JDBC Row Set
  * @param connection   the [[JDBCConnection connection]]
  * @param databaseName the database name
  * @param schemaName   the schema name
  * @param tableName    the table name
  * @param columns      the collection of [[TableColumn columns]]
  * @param rows         the row data
  * @param __ids        the collection of row identifiers
  */
class JDBCRowSet(connection: JDBCConnection,
                     databaseName: String,
                     schemaName: String,
                     tableName: String,
                     columns: Seq[TableColumn],
                     rows: Seq[Seq[Option[Any]]],
                     __ids: Seq[ROWID])  {
  private val logger = LoggerFactory.getLogger(getClass)
  private val matrix: Array[Option[Any]] = rows.flatten.toArray
  private var rowIndex = uninited

  def first(): Boolean = {
    rowIndex = uninited
    isValidRow
  }

  def last(): Boolean = {
    rowIndex = (rows.length - 2) max uninited
    isValidRow
  }

  def getColumnValue[T](columnIndex: Int): T = {
    getColumnValueOpt[T](columnIndex).getOrElse(null.asInstanceOf[T])
  }

  def getColumnValue[T](columnLabel: String): T = {
    getColumnValueOpt[T](columnLabel).getOrElse(null.asInstanceOf[T])
  }

  def getColumnValueOpt[T](columnIndex: Int): Option[T] = {
    val offset = getOffset(columnIndex)

    // lookup the value by column name
    val column = columns(columnIndex - 1)
    val columnType = ColumnTypes.withName(column.columnType)
    val rawValue_? = matrix(offset)
    val value = rawValue_?.flatMap(rv => safeCast[T](Codec.convertTo(rv, columnType)))
    logger.debug(s"getColumnValueOpt($columnIndex) => $value [rowIndex=$rowIndex]")
    value
  }

  def getColumnValueOpt[T](columnLabel: String): Option[T] = {
    val index = columns.indexWhere(_.name == columnLabel)
    validateColumnIndex(index, columnLabel)
    val value = getColumnValueOpt[T](columnIndex = index + 1)
    logger.debug(s"""getColumnValueOpt("$columnLabel") => $value [rowIndex=$rowIndex]""")
    value
  }

  def getRowId(columnIndex: Int): RowId = {
    validateColumnIndex(columnIndex)
    if (__ids.nonEmpty) JDBCRowId(__ids(rowIndex)) else null
  }

  def getRowId(columnLabel: String): RowId = {
    validateColumnIndex(columnIndex = columns.indexWhere(_.name == columnLabel), columnLabel)
    if (__ids.nonEmpty) JDBCRowId(__ids(rowIndex)) else null
  }

  def getRowNumber: Int = rowIndex + 1

  def isBeforeFirst: Boolean = rowIndex < 0

  def isAfterLast: Boolean = rowIndex >= rows.length

  def isEmpty: Boolean = rows.isEmpty

  def isFirst: Boolean = rowIndex == 0

  def isLast: Boolean = rowIndex == rows.length - 1

  def nonEmpty: Boolean = rows.nonEmpty

  def length: Int = rows.length

  def next(): Boolean = {
    rowIndex += 1
    rowIndex < rows.length
  }

  def previous(): Boolean = {
    if (rowIndex < 0) false else {
      rowIndex -= 1
      true
    }
  }

  def beforeFirst(): Unit = rowIndex = uninited

  def afterLast(): Unit = rows.length

  def absolute(row: Int): Boolean = ???

  def relative(rows: Int): Boolean = ???

  def cancelRowUpdates(): Unit = refreshRow()

  def moveToInsertRow(): Unit = ???

  def moveToCurrentRow(): Unit = rowIndex = rows.length - 1

  def insertRow(): Unit = connection.client.insertRow(databaseName, tableName, constructRow)

  def updateRow(): Unit = connection.client.replaceRow(databaseName, tableName, __id(), constructRow)

  def deleteRow(): Unit = connection.client.deleteRow(databaseName, tableName, __id())

  def refreshRow(): Unit = {
    val refreshedRow = for {
      row <- connection.client.getRow(databaseName, tableName, __id()).toArray
      name <- columns.map(_.name)
    } yield row.get(name)

    // overwrite the slice
    val p0 = rowIndex * columns.length
    val p1 = (rowIndex + 1) * columns.length
    System.arraycopy(refreshedRow, 0, matrix, p0, p1 - p0)
  }

  def update(columnIndex: Int, value: Any): Unit = {
    val value_? = value match {
      case aValue@Some(_) => aValue
      case None => None
      case v => Some(v)
    }
    matrix(getOffset(columnIndex)) = value_?
  }

  def update(columnLabel: String, value: Any): Unit = {
    val index = columns.indexWhere(_.name == columnLabel)
    validateColumnIndex(index, columnLabel)
    update(columnIndex = index + 1, value)
  }

  def updateNull(columnIndex: Int): Unit = matrix(getOffset(columnIndex)) = None

  def updateNull(columnLabel: String): Unit = {
    val index = columns.indexWhere(_.name == columnLabel)
    validateColumnIndex(index, columnLabel)
    matrix(getOffset(columnIndex = index + 1)) = None
  }

  private def constructRow: KeyValues = {
    val p0 = rowIndex * columns.length
    val p1 = (rowIndex + 1) * columns.length
    KeyValues((for {
      n <- p0 until p1
      name = columns(n - p0).name
      value <- matrix(n)
    } yield name -> value): _*)
  }

  private def getOffset(columnIndex: Int): Int = {
    assert(rowIndex >= 0 && rowIndex < rows.length, s"Row index is out of range ($rowIndex)")
    validateColumnIndex(columnIndex)
    val offset = rowIndex * columns.length + (columnIndex - 1)
    assert(offset >= 0 && offset < rows.length * columns.length, s"Invalid offset ($offset)")
    offset
  }

  private def __id(columnIndex: Int = 1): ROWID = ByteBuffer.wrap(getRowId(columnIndex).getBytes).getRowID

  private def isValidRow: Boolean = rowIndex + 1 < rows.length

  private def validateColumnIndex(columnIndex: Int): Unit = {
    assert(columnIndex > 0 && columnIndex <= columns.length, s"Column index is out of range ($columnIndex)")
  }

  private def validateColumnIndex(columnIndex: Int, columnLabel: String): Unit = {
    assert(columnIndex >= 0 && columnIndex < columns.length, s"Column '$columnLabel' does not exist")
  }

}

/**
  * JDBC Row Set Companion
  */
object JDBCRowSet {
  private val uninited = -1

  /**
    * Creates a new JDBC Row Set
    * @param connection   the [[JDBCConnection connection]]
    * @param databaseName the database name
    * @param schemaName   the schema name
    * @param tableName    the table name
    * @param columns      the collection of [[TableColumn columns]]
    * @param rows         the row data
    * @param __ids        the collection of row identifiers
    */
  def apply(connection: JDBCConnection,
                       databaseName: String,
                       schemaName: String,
                       tableName: String,
                       columns: Seq[TableColumn],
                       rows: Seq[Seq[Option[Any]]],
                       __ids: Seq[ROWID]): JDBCRowSet = {
    new JDBCRowSet(connection, databaseName, schemaName, tableName, columns, rows, __ids)
  }

}