package com.qwery.database
package jdbc

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Array => SQLArray, _}
import java.util.Calendar

import com.qwery.database.jdbc.JDBCPreparedStatement._
import com.qwery.database.types.QxAny.{RichInputStream, RichReader}

import scala.beans.BeanProperty

/**
 * Qwery JDBC Prepared Statement
 * @param connection the [[JDBCConnection connection]]
 * @param sql        the SQL query
 */
class JDBCPreparedStatement(@BeanProperty connection: JDBCConnection, sql: String)
  extends JDBCStatement(connection) with PreparedStatement {
  protected var batches: List[List[Any]] = Nil
  protected val indices: List[Int] = parseSQLTemplate(sql)

  @BeanProperty val parameterMetaData = new JDBCParameterMetaData()

  override def addBatch(): Unit = batches = parameterMetaData.getParameters :: batches

  override def clearBatch(): Unit = batches = Nil

  override def clearParameters(): Unit = parameterMetaData.clear()

  override def execute(): Boolean = {
    val _sql = populateSQLTemplate(sql, indices, parameterMetaData.getParameters)
    val outcome = connection.client.executeQuery(connection.getCatalog, _sql)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.count
    outcome.rows.nonEmpty
  }

  override def executeBatch(): Array[Int] = {
    val outcome = (batches.reverse map { params =>
      val _sql = populateSQLTemplate(sql, indices, params)
      val outcome = connection.client.executeQuery(connection.getCatalog, _sql)
      outcome.count
    }).toArray
    clearBatch()
    outcome
  }

  override def executeQuery(): ResultSet = {
    val _sql = populateSQLTemplate(sql, indices, parameterMetaData.getParameters)
    val outcome = connection.client.executeQuery(connection.getCatalog, _sql)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.count
    resultSet
  }

  override def executeUpdate(): Int = {
    val _sql = populateSQLTemplate(sql, indices, parameterMetaData.getParameters)
    val outcome = connection.client.executeQuery(connection.getCatalog, _sql)
    resultSet = JDBCResultSet(connection, outcome)
    updateCount = outcome.count
    updateCount
  }

  override def setNull(parameterIndex: Int, sqlType: Int): Unit = parameterMetaData(parameterIndex) = _null(sqlType)

  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = parameterMetaData(parameterIndex) = x

  override def setByte(parameterIndex: Int, x: Byte): Unit = parameterMetaData(parameterIndex) = x

  override def setShort(parameterIndex: Int, x: Short): Unit = parameterMetaData(parameterIndex) = x

  override def setInt(parameterIndex: Int, x: Int): Unit = parameterMetaData(parameterIndex) = x

  override def setLong(parameterIndex: Int, x: Long): Unit = parameterMetaData(parameterIndex) = x

  override def setFloat(parameterIndex: Int, x: Float): Unit = parameterMetaData(parameterIndex) = x

  override def setDouble(parameterIndex: Int, x: Double): Unit = parameterMetaData(parameterIndex) = x

  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = parameterMetaData(parameterIndex) = x

  override def setString(parameterIndex: Int, x: String): Unit = parameterMetaData(parameterIndex) = x

  override def setBytes(parameterIndex: Int, x: Array[Byte]): Unit = parameterMetaData(parameterIndex) = x

  override def setDate(parameterIndex: Int, x: Date): Unit = parameterMetaData(parameterIndex) = x

  override def setTime(parameterIndex: Int, x: Time): Unit = parameterMetaData(parameterIndex) = x

  override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = parameterMetaData(parameterIndex) = x

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x

  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = parameterMetaData(parameterIndex) = x

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit = parameterMetaData(parameterIndex) = _cast(x, targetSqlType)

  override def setObject(parameterIndex: Int, x: Any): Unit = parameterMetaData(parameterIndex) = x

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit = parameterMetaData(parameterIndex) = reader.toClob(length)

  override def setRef(parameterIndex: Int, x: Ref): Unit = parameterMetaData(parameterIndex) = x

  override def setBlob(parameterIndex: Int, x: Blob): Unit = parameterMetaData(parameterIndex) = x

  override def setClob(parameterIndex: Int, x: Clob): Unit = parameterMetaData(parameterIndex) = x

  override def setArray(parameterIndex: Int, x: SQLArray): Unit = parameterMetaData(parameterIndex) = x

  override def getMetaData: ResultSetMetaData = Option(resultSet).map(_.getMetaData).orNull

  override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit = parameterMetaData(parameterIndex) = x

  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = parameterMetaData(parameterIndex) = _null(sqlType)

  override def setURL(parameterIndex: Int, x: URL): Unit = parameterMetaData(parameterIndex) = x

  override def setRowId(parameterIndex: Int, x: RowId): Unit = parameterMetaData(parameterIndex) = x

  override def setNString(parameterIndex: Int, value: String): Unit = setString(parameterIndex, value)

  override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit = setCharacterStream(parameterIndex, value, length)

  override def setNClob(parameterIndex: Int, value: NClob): Unit = setClob(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit = reader.toClob(length)

  override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = inputStream.toBlob(length)

  override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit = setClob(parameterIndex, reader, length)

  override def setSQLXML(parameterIndex: Int, xmlObject: SQLXML): Unit = parameterMetaData(parameterIndex) = xmlObject

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit = parameterMetaData(parameterIndex) = _cast(x, targetSqlType, scaleOrLength)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = x.toBlob(length)

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = parameterMetaData(parameterIndex) = x.toBlob(length)

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit = parameterMetaData(parameterIndex) = reader.toClob(length)

  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = parameterMetaData(parameterIndex) = x.toBlob

  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = parameterMetaData(parameterIndex) = x.toBlob

  override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit = parameterMetaData(parameterIndex) = reader.toClob

  override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit = setCharacterStream(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader): Unit = parameterMetaData(parameterIndex) = reader.toClob

  override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = parameterMetaData(parameterIndex) = inputStream.toBlob

  override def setNClob(parameterIndex: Int, reader: Reader): Unit = setClob(parameterIndex, reader)

}

/**
 * JDBC Prepared Statement Companion
 */
object JDBCPreparedStatement {

  def _cast(value: Any, targetSqlType: Int, scale: Int = 0): Option[Any] = {
    Option(Codec.convertTo(value, convertSqlToColumnType(targetSqlType)))
  }

  def _null(sqlType: Int): Option[Any] = None

  def parseSQLTemplate(sql: String): List[Int] = {
    var params: List[Int] = Nil
    var pos = -1
    do {
      pos = sql.indexOf('?', pos + 1)
      if (pos != -1) params = pos :: params
    } while (pos != -1)
    params.reverse
  }

  def populateSQLTemplate(sql: String, indices: List[Int], params: List[Any]): String = {
    assert(indices.size == params.size, s"Parameter length mismatch: ${indices.size} != ${params.size}")
    val (newSQL, _) = indices.reverse.foldLeft(new StringBuilder(sql) -> (indices.size - 1)) { case ((sb, index), pos) =>
      sb.replace(pos, pos + 1, params(index) match {
        case n if n.toString.matches("^-?[0-9]\\d*(\\.\\d+)?$") => n.toString
        case x => s"'$x'"
      })
      (sb, index - 1)
    }
    newSQL.toString()
  }

}