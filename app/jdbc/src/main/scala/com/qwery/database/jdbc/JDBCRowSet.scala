package com.qwery.database
package jdbc

import java.nio.ByteBuffer
import java.sql.RowId

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.models.TableColumn

/**
 * Qwery JDBC Row Set
 * @param connection the [[JDBCConnection connection]]
 * @param tableName  the table name
 * @param columns    the collection of [[TableColumn columns]]
 * @param rows       the row data
 */
class JDBCRowSet(connection: JDBCConnection,
                 tableName: String,
                 columns: Seq[TableColumn],
                 rows: Seq[Seq[Option[Any]]],
                 __ids: Seq[ROWID]) {
  private val matrix: Array[Option[Any]] = rows.flatten.toArray
  private var rowIndex = -1

  def first(): Boolean = {
    rowIndex = -1
    true
  }

  def last(): Boolean = {
    rowIndex = rows.length - 2
    true
  }

  def getColumnValue[T](columnIndex: Int): T = {
    getColumnValueOpt[T](columnIndex).getOrElse(die(s"No value was found for column index $columnIndex"))
  }

  def getColumnValue[T](columnLabel: String): T = {
    getColumnValueOpt[T](columnLabel).getOrElse(die(s"No value was found for column name '$columnLabel''"))
  }

  def getColumnValueOpt[T](columnIndex: Int): Option[T] = {
    val offset = getOffset(columnIndex)

    // lookup the value by column name
    val column = columns(columnIndex - 1)
    val columnType = ColumnTypes.withName(column.columnType)
    val rawValue_? = matrix(offset)
    rawValue_?.flatMap(rv => safeCast[T](Codec.convertTo(rv, columnType)))
  }

  def getColumnValueOpt[T](columnLabel: String): Option[T] = {
    val index = columns.indexWhere(_.name == columnLabel)
    assert(index >= 0 && index < columns.length, s"Column '$columnLabel' does not exist")
    getColumnValueOpt[T](columnIndex = index + 1)
  }

  def getRowId(columnIndex: Int): RowId = if (__ids.nonEmpty) JDBCRowId(__ids(rowIndex)) else null

  def getRowId(columnLabel: String): RowId = if (__ids.nonEmpty) JDBCRowId(__ids(rowIndex)) else null

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

  def beforeFirst(): Unit = ???

  def afterLast(): Unit = ???

  def absolute(row: Int): Boolean = ???

  def relative(rows: Int): Boolean = ???

  def cancelRowUpdates(): Unit = refreshRow()

  def moveToInsertRow(): Unit = ???

  def moveToCurrentRow(): Unit = rowIndex = rows.length - 1

  def insertRow(): Unit = connection.client.insertRow(connection.database, tableName, constructRow)

  def updateRow(): Unit = connection.client.replaceRow(connection.database, tableName, __id(), constructRow)

  def deleteRow(): Unit = connection.client.deleteRow(connection.database, tableName, __id())

  def refreshRow(): Unit = {
    val refreshedRow = for {
      row <- connection.client.getRow(connection.database, tableName, __id()).toArray
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
    assert(index >= 0 && index < columns.length, s"Column '$columnLabel' does not exist")
    update(columnIndex = index + 1, value)
  }

  def updateNull(columnIndex: Int): Unit = matrix(getOffset(columnIndex)) = None

  def updateNull(columnLabel: String): Unit = {
    val index = columns.indexWhere(_.name == columnLabel)
    assert(index >= 0 && index < columns.length, s"Column '$columnLabel' does not exist")
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
    assert(columnIndex > 0 && columnIndex <= columns.length, s"Column index is out of range ($columnIndex)")
    val offset = rowIndex * columns.length + (columnIndex - 1)
    assert(offset >= 0 && offset < rows.length * columns.length, s"Invalid offset ($offset)")
    offset
  }

  private def __id(columnIndex: Int = 0): ROWID = ByteBuffer.wrap(getRowId(columnIndex).getBytes).getRowID
  
}