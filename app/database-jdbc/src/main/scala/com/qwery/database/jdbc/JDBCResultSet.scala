package com.qwery.database
package jdbc

import com.qwery.database.models._
import com.qwery.database.types.QxAny.{RichInputStream, RichReader}
import com.qwery.util.OptionHelper.OptionEnrichment

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Array => SQLArray, _}
import java.util
import java.util.{Calendar, UUID}
import scala.beans.{BeanProperty, BooleanBeanProperty}

/**
 * Qwery JDBC Result Set
 * @param connection   the [[JDBCConnection connection]]
 * @param databaseName the database name
 * @param schemaName   the schema name
 * @param tableName    the table name
 * @param columns      the [[TableColumn table columns]]
 * @param data         the collection of row data
 * @param __ids        the collection of row identifiers
 */
class JDBCResultSet(connection: JDBCConnection,
                    databaseName: String,
                    schemaName: String,
                    tableName: String,
                    columns: Seq[TableColumn],
                    data: Seq[Seq[Option[Any]]],
                    __ids: Seq[ROWID] = Nil) extends ResultSet with JDBCWrapper {
  private var isRowUpdated: Boolean = false
  private var isRowInserted: Boolean = false
  private var isRowDeleted: Boolean = false
  private val sqlWarning = new SQLWarning()
  private val rows = new JDBCRowSet(connection, databaseName, schemaName, tableName, columns, data, __ids)

  @BooleanBeanProperty var closed: Boolean = false
  @BeanProperty val concurrency: Int = ResultSet.CONCUR_UPDATABLE
  @BeanProperty val cursorName: String = UUID.randomUUID().toString
  @BeanProperty var fetchDirection: Int = ResultSet.FETCH_FORWARD
  @BeanProperty var fetchSize: Int = 20
  @BeanProperty val holdability: Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
  @BeanProperty val metaData = new JDBCResultSetMetaData(databaseName, schemaName, tableName, columns)
  @BeanProperty val `type`: Int = ResultSet.TYPE_SCROLL_SENSITIVE

  override def next(): Boolean = rows.next()

  override def close(): Unit = closed = true

  override def wasNull(): Boolean = rows.isEmpty

  override def getString(columnIndex: Int): String = rows.getColumnValueOpt[String](columnIndex).orNull

  override def getBoolean(columnIndex: Int): Boolean = rows.getColumnValue[Boolean](columnIndex)

  override def getByte(columnIndex: Int): Byte = rows.getColumnValue[Byte](columnIndex)

  override def getShort(columnIndex: Int): Short = rows.getColumnValue[Short](columnIndex)

  override def getInt(columnIndex: Int): Int = rows.getColumnValue[Int](columnIndex)

  override def getLong(columnIndex: Int): Long = rows.getColumnValue[Long](columnIndex)

  override def getFloat(columnIndex: Int): Float = rows.getColumnValue[Float](columnIndex)

  override def getDouble(columnIndex: Int): Double = rows.getColumnValue[Double](columnIndex)

  override def getBigDecimal(columnIndex: Int, scale: Int): java.math.BigDecimal = rows.getColumnValueOpt[java.math.BigDecimal](columnIndex).orNull

  override def getBytes(columnIndex: Int): Array[Byte] = rows.getColumnValueOpt[Array[Byte]](columnIndex).orNull

  override def getDate(columnIndex: Int): Date = {
    rows.getColumnValueOpt[java.util.Date](columnIndex).map(d => new Date(d.getTime)).orNull
  }

  override def getTime(columnIndex: Int): Time = {
    rows.getColumnValueOpt[java.util.Date](columnIndex).map(d => new Time(d.getTime)).orNull
  }

  override def getTimestamp(columnIndex: Int): Timestamp = {
    rows.getColumnValueOpt[java.util.Date](columnIndex).map(d => new Timestamp(d.getTime)).orNull
  }

  override def getAsciiStream(columnIndex: Int): InputStream = getClob(columnIndex).getAsciiStream

  override def getUnicodeStream(columnIndex: Int): InputStream = getClob(columnIndex).getAsciiStream

  override def getBinaryStream(columnIndex: Int): InputStream = getBlob(columnIndex).getBinaryStream

  override def getString(columnLabel: String): String = rows.getColumnValueOpt[String](columnLabel).orNull

  override def getBoolean(columnLabel: String): Boolean = rows.getColumnValue[Boolean](columnLabel)

  override def getByte(columnLabel: String): Byte = rows.getColumnValue[Byte](columnLabel)

  override def getShort(columnLabel: String): Short = rows.getColumnValue[Short](columnLabel)

  override def getInt(columnLabel: String): Int = rows.getColumnValue[Int](columnLabel)

  override def getLong(columnLabel: String): Long = rows.getColumnValue[Long](columnLabel)

  override def getFloat(columnLabel: String): Float = rows.getColumnValue[Float](columnLabel)

  override def getDouble(columnLabel: String): Double = rows.getColumnValue[Double](columnLabel)

  override def getBigDecimal(columnLabel: String, scale: Int): java.math.BigDecimal = rows.getColumnValueOpt[java.math.BigDecimal](columnLabel).orNull

  override def getBytes(columnLabel: String): Array[Byte] = rows.getColumnValueOpt[Array[Byte]](columnLabel).orNull

  override def getTimestamp(columnLabel: String): Timestamp = rows.getColumnValueOpt[Timestamp](columnLabel).orNull

  override def getAsciiStream(columnLabel: String): InputStream = getClob(columnLabel).getAsciiStream

  override def getUnicodeStream(columnLabel: String): InputStream = getClob(columnLabel).getAsciiStream

  override def getBinaryStream(columnLabel: String): InputStream = getBlob(columnLabel).getBinaryStream

  override def getWarnings: SQLWarning = sqlWarning

  override def clearWarnings(): Unit = ()

  override def getObject(columnIndex: Int): AnyRef = rows.getColumnValueOpt[AnyRef](columnIndex).orNull

  override def getObject(columnLabel: String): AnyRef = rows.getColumnValueOpt[AnyRef](columnLabel).orNull

  override def findColumn(columnLabel: String): Int = columns.indexWhere(_.name == columnLabel) match {
    case -1 => throw new SQLException(s"Column '$columnLabel' not found'")
    case index => index + 1
  }

  override def getCharacterStream(columnIndex: Int): Reader = getClob(columnIndex).getCharacterStream

  override def getCharacterStream(columnLabel: String): Reader = getClob(columnLabel).getCharacterStream

  override def getBigDecimal(columnIndex: Int): java.math.BigDecimal = rows.getColumnValueOpt[java.math.BigDecimal](columnIndex).orNull

  override def getBigDecimal(columnLabel: String): java.math.BigDecimal = rows.getColumnValueOpt[java.math.BigDecimal](columnLabel).orNull

  override def isBeforeFirst: Boolean = rows.isBeforeFirst

  override def isAfterLast: Boolean = rows.isAfterLast

  override def isFirst: Boolean = rows.isFirst

  override def isLast: Boolean = rows.isLast

  override def beforeFirst(): Unit = rows.beforeFirst()

  override def afterLast(): Unit = rows.afterLast()

  override def first(): Boolean = rows.first()

  override def last(): Boolean = rows.last()

  override def getRow: Int = rows.getRowNumber

  override def absolute(row: Int): Boolean = rows.absolute(row)

  override def relative(rowDelta: Int): Boolean = rows.relative(rowDelta)

  override def previous(): Boolean = rows.previous()

  override def rowUpdated(): Boolean = isRowUpdated

  override def rowInserted(): Boolean = isRowInserted

  override def rowDeleted(): Boolean = isRowDeleted

  override def updateNull(columnIndex: Int): Unit = rows.updateNull(columnIndex)

  override def updateBoolean(columnIndex: Int, x: Boolean): Unit = rows.update(columnIndex, Some(x))

  override def updateByte(columnIndex: Int, x: Byte): Unit = rows.update(columnIndex, Some(x))

  override def updateShort(columnIndex: Int, x: Short): Unit = rows.update(columnIndex, Some(x))

  override def updateInt(columnIndex: Int, x: Int): Unit = rows.update(columnIndex, Some(x))

  override def updateLong(columnIndex: Int, x: Long): Unit = rows.update(columnIndex, Some(x))

  override def updateFloat(columnIndex: Int, x: Float): Unit = rows.update(columnIndex, Some(x))

  override def updateDouble(columnIndex: Int, x: Double): Unit = rows.update(columnIndex, Some(x))

  override def updateBigDecimal(columnIndex: Int, x: java.math.BigDecimal): Unit = rows.update(columnIndex, Option(x))

  override def updateString(columnIndex: Int, x: String): Unit = rows.update(columnIndex, Option(x))

  override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit = rows.update(columnIndex, Option(x))

  override def updateDate(columnIndex: Int, x: Date): Unit = rows.update(columnIndex, Option(x))

  override def updateTime(columnIndex: Int, x: Time): Unit = rows.update(columnIndex, Option(x))

  override def updateTimestamp(columnIndex: Int, x: Timestamp): Unit = rows.update(columnIndex, Option(x))

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Int): Unit = rows.update(columnIndex, x.toBlob(length))

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Int): Unit = rows.update(columnIndex, x.toBlob(length))

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Int): Unit = rows.update(columnIndex, x.toClob(length))

  override def updateObject(columnIndex: Int, x: Any, scaleOrLength: Int): Unit = rows.update(columnIndex, Option(x)) // TODO scaleOrLength

  override def updateObject(columnIndex: Int, x: Any): Unit = rows.update(columnIndex, Option(x))

  override def updateNull(columnLabel: String): Unit = rows.updateNull(columnLabel)

  override def updateBoolean(columnLabel: String, x: Boolean): Unit = rows.update(columnLabel, Some(x))

  override def updateByte(columnLabel: String, x: Byte): Unit = rows.update(columnLabel, Some(x))

  override def updateShort(columnLabel: String, x: Short): Unit = rows.update(columnLabel, Some(x))

  override def updateInt(columnLabel: String, x: Int): Unit = rows.update(columnLabel, Some(x))

  override def updateLong(columnLabel: String, x: Long): Unit = rows.update(columnLabel, Some(x))

  override def updateFloat(columnLabel: String, x: Float): Unit = rows.update(columnLabel, Some(x))

  override def updateDouble(columnLabel: String, x: Double): Unit = rows.update(columnLabel, Some(x))

  override def updateBigDecimal(columnLabel: String, x: java.math.BigDecimal): Unit = rows.update(columnLabel, Option(x))

  override def updateString(columnLabel: String, x: String): Unit = rows.update(columnLabel, Option(x))

  override def updateBytes(columnLabel: String, x: Array[Byte]): Unit = rows.update(columnLabel, Option(x))

  override def updateDate(columnLabel: String, x: Date): Unit = rows.update(columnLabel, Option(x))

  override def updateTime(columnLabel: String, x: Time): Unit = rows.update(columnLabel, Option(x))

  override def updateTimestamp(columnLabel: String, x: Timestamp): Unit = rows.update(columnLabel, Option(x))

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Int): Unit = rows.update(columnLabel, x.toBlob(length))

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Int): Unit = rows.update(columnLabel, x.toBlob(length))

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Int): Unit = rows.update(columnLabel, reader.toClob(length))

  override def updateObject(columnLabel: String, x: Any, scaleOrLength: Int): Unit = rows.update(columnLabel, Option(x)) // TODO scaleOrLength

  override def updateObject(columnLabel: String, x: Any): Unit = rows.update(columnLabel, Option(x))

  override def insertRow(): Unit = {
    rows.insertRow()
    isRowInserted = true
  }

  override def updateRow(): Unit = {
    rows.updateRow()
    isRowUpdated = true
  }

  override def deleteRow(): Unit = {
    rows.deleteRow()
    isRowDeleted = true
  }

  override def refreshRow(): Unit = rows.refreshRow()

  override def cancelRowUpdates(): Unit = rows.cancelRowUpdates()

  override def moveToInsertRow(): Unit = rows.moveToInsertRow()

  override def moveToCurrentRow(): Unit = rows.moveToCurrentRow()

  override def getStatement: Statement = new JDBCStatement(connection)

  override def getObject(columnIndex: Int, map: util.Map[String, Class[_]]): AnyRef = rows.getColumnValueOpt[AnyRef](columnIndex).orNull

  override def getRef(columnIndex: Int): Ref = rows.getColumnValueOpt[Ref](columnIndex).orNull

  override def getBlob(columnIndex: Int): Blob = rows.getColumnValueOpt[Blob](columnIndex).orNull

  override def getClob(columnIndex: Int): Clob = rows.getColumnValueOpt[Clob](columnIndex).orNull

  override def getArray(columnIndex: Int): SQLArray = rows.getColumnValueOpt[SQLArray](columnIndex).orNull

  override def getObject(columnLabel: String, map: util.Map[String, Class[_]]): AnyRef = rows.getColumnValue[AnyRef](columnLabel)

  override def getRef(columnLabel: String): Ref = rows.getColumnValueOpt[Ref](columnLabel).orNull

  override def getBlob(columnLabel: String): Blob = rows.getColumnValueOpt[Blob](columnLabel).orNull

  override def getClob(columnLabel: String): Clob = rows.getColumnValueOpt[Clob](columnLabel).orNull

  override def getArray(columnLabel: String): SQLArray = rows.getColumnValueOpt[SQLArray](columnLabel).orNull

  override def getDate(columnIndex: Int, cal: Calendar): Date = getDate(columnIndex)

  override def getDate(columnLabel: String, cal: Calendar): Date = getDate(columnLabel)

  override def getTime(columnIndex: Int, cal: Calendar): Time = getTime(columnIndex)

  override def getTime(columnLabel: String, cal: Calendar): Time = getTime(columnLabel)

  override def getTimestamp(columnIndex: Int, cal: Calendar): Timestamp = getTimestamp(columnIndex)

  override def getTimestamp(columnLabel: String, cal: Calendar): Timestamp = getTimestamp(columnLabel)

  override def getURL(columnIndex: Int): URL = rows.getColumnValueOpt[URL](columnIndex).orNull

  override def getURL(columnLabel: String): URL = rows.getColumnValueOpt[URL](columnLabel).orNull

  override def updateRef(columnIndex: Int, x: Ref): Unit = rows.update(columnIndex, x)

  override def updateRef(columnLabel: String, x: Ref): Unit = rows.update(columnLabel, x)

  override def updateBlob(columnIndex: Int, x: Blob): Unit = rows.update(columnIndex, x)

  override def updateBlob(columnLabel: String, x: Blob): Unit = rows.update(columnLabel, x)

  override def updateClob(columnIndex: Int, x: Clob): Unit = rows.update(columnIndex, x)

  override def updateClob(columnLabel: String, x: Clob): Unit = rows.update(columnLabel, x)

  override def updateArray(columnIndex: Int, x: SQLArray): Unit = rows.update(columnIndex, x)

  override def updateArray(columnLabel: String, x: SQLArray): Unit = rows.update(columnLabel, x)

  override def getRowId(columnIndex: Int): RowId = rows.getRowId(columnIndex)

  override def getRowId(columnLabel: String): RowId = rows.getRowId(columnLabel)

  override def updateRowId(columnIndex: Int, x: RowId): Unit = rows.update(columnIndex, x)

  override def updateRowId(columnLabel: String, x: RowId): Unit = rows.update(columnLabel, x)

  override def updateNString(columnIndex: Int, nString: String): Unit = updateString(columnIndex, nString)

  override def updateNString(columnLabel: String, nString: String): Unit = updateString(columnLabel, nString)

  override def updateNClob(columnIndex: Int, nClob: NClob): Unit = updateClob(columnIndex, nClob)

  override def updateNClob(columnLabel: String, nClob: NClob): Unit = updateClob(columnLabel, nClob)

  override def getNClob(columnIndex: Int): NClob = rows.getColumnValueOpt[NClob](columnIndex).orNull

  override def getNClob(columnLabel: String): NClob = rows.getColumnValueOpt[NClob](columnLabel).orNull

  override def getSQLXML(columnIndex: Int): SQLXML = rows.getColumnValueOpt[SQLXML](columnIndex).orNull

  override def getSQLXML(columnLabel: String): SQLXML = rows.getColumnValueOpt[SQLXML](columnLabel).orNull

  override def updateSQLXML(columnIndex: Int, xmlObject: SQLXML): Unit = rows.update(columnIndex, xmlObject)

  override def updateSQLXML(columnLabel: String, xmlObject: SQLXML): Unit = rows.update(columnLabel, xmlObject)

  override def getNString(columnIndex: Int): String = getString(columnIndex)

  override def getNString(columnLabel: String): String = getString(columnLabel)

  override def getNCharacterStream(columnIndex: Int): Reader = getCharacterStream(columnIndex)

  override def getNCharacterStream(columnLabel: String): Reader = getCharacterStream(columnLabel)

  override def updateNCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = updateCharacterStream(columnIndex, x, length)

  override def updateNCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = updateCharacterStream(columnLabel, reader, length)

  override def updateAsciiStream(columnIndex: Int, x: InputStream, length: Long): Unit = rows.update(columnIndex, x.toBlob(length))

  override def updateBinaryStream(columnIndex: Int, x: InputStream, length: Long): Unit = rows.update(columnIndex, x.toBlob(length))

  override def updateCharacterStream(columnIndex: Int, x: Reader, length: Long): Unit = rows.update(columnIndex, x.toClob(length))

  override def updateAsciiStream(columnLabel: String, x: InputStream, length: Long): Unit = rows.update(columnLabel, x.toBlob(length))

  override def updateBinaryStream(columnLabel: String, x: InputStream, length: Long): Unit = rows.update(columnLabel, x.toBlob(length))

  override def updateCharacterStream(columnLabel: String, reader: Reader, length: Long): Unit = rows.update(columnLabel, reader.toClob(length))

  override def updateBlob(columnIndex: Int, inputStream: InputStream, length: Long): Unit = rows.update(columnIndex, inputStream.toBlob(length))

  override def updateBlob(columnLabel: String, inputStream: InputStream, length: Long): Unit = rows.update(columnLabel, inputStream.toBlob(length))

  override def updateClob(columnIndex: Int, reader: Reader, length: Long): Unit = rows.update(columnIndex, reader.toClob(length))

  override def updateClob(columnLabel: String, reader: Reader, length: Long): Unit = rows.update(columnLabel, reader.toClob(length))

  override def updateNClob(columnIndex: Int, reader: Reader, length: Long): Unit = updateClob(columnIndex, reader, length)

  override def updateNClob(columnLabel: String, reader: Reader, length: Long): Unit = updateClob(columnLabel, reader, length)

  override def updateNCharacterStream(columnIndex: Int, x: Reader): Unit = updateCharacterStream(columnIndex, x)

  override def updateNCharacterStream(columnLabel: String, reader: Reader): Unit = updateCharacterStream(columnLabel, reader)

  override def updateAsciiStream(columnIndex: Int, x: InputStream): Unit = rows.update(columnIndex, x.toBlob)

  override def updateBinaryStream(columnIndex: Int, x: InputStream): Unit = rows.update(columnIndex, x.toBlob)

  override def updateCharacterStream(columnIndex: Int, x: Reader): Unit = rows.update(columnIndex, x.toClob)

  override def updateAsciiStream(columnLabel: String, x: InputStream): Unit = rows.update(columnLabel, x.toBlob)

  override def updateBinaryStream(columnLabel: String, x: InputStream): Unit = rows.update(columnLabel, x.toBlob)

  override def updateCharacterStream(columnLabel: String, reader: Reader): Unit = rows.update(columnLabel, reader.toClob)

  override def updateBlob(columnIndex: Int, inputStream: InputStream): Unit = rows.update(columnIndex, inputStream.toBlob)

  override def updateBlob(columnLabel: String, inputStream: InputStream): Unit = rows.update(columnLabel, inputStream.toBlob)

  override def updateClob(columnIndex: Int, reader: Reader): Unit = rows.update(columnIndex, reader.toClob)

  override def updateClob(columnLabel: String, reader: Reader): Unit = rows.update(columnLabel, reader.toClob)

  override def updateNClob(columnIndex: Int, reader: Reader): Unit = updateClob(columnIndex, reader)

  override def updateNClob(columnLabel: String, reader: Reader): Unit = updateClob(columnLabel, reader)

  override def getObject[T](columnIndex: Int, `type`: Class[T]): T = rows.getColumnValueOpt[T](columnIndex).getOrElse(null.asInstanceOf[T])

  override def getDate(columnLabel: String): Date = rows.getColumnValue[Date](columnLabel)

  override def getTime(columnLabel: String): Time = rows.getColumnValue[Time](columnLabel)

  override def getObject[T](columnLabel: String, `type`: Class[T]): T = rows.getColumnValueOpt[T](columnLabel).getOrElse(null.asInstanceOf[T])

}

/**
 * Qwery Result Set Companion
 */
object JDBCResultSet {

  /**
   * Creates a new [[JDBCResultSet Qwery result set]]
   * @param queryResult the [[QueryResult]]
   * @return a new [[JDBCResultSet Qwery result set]]
   */
  def apply(connection: JDBCConnection, queryResult: QueryResult) = new JDBCResultSet(
    connection = connection,
    databaseName = queryResult.ref.databaseName || connection.catalog,
    schemaName = queryResult.ref.schemaName || connection.schema,
    tableName = queryResult.ref.name,
    columns = queryResult.columns,
    data = queryResult.rows,
    __ids = queryResult.__ids
  )

}