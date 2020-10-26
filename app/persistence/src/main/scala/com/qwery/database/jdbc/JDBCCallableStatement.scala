package com.qwery.database.jdbc

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql.{Blob, CallableStatement, Clob, Date, NClob, Ref, RowId, SQLXML, Time, Timestamp, Array => SQLArray}
import java.util
import java.util.Calendar

import com.qwery.database.jdbc.JDBCPreparedStatement._
import com.qwery.database.types.QxAny.{RichInputStream, RichReader}

import scala.beans.BeanProperty
import scala.collection.concurrent.TrieMap

/**
 * Qwery Callable Statement
 * @param connection the [[JDBCConnection connection]]
 * @param sql        the SQL query
 */
class JDBCCallableStatement(@BeanProperty connection: JDBCConnection, sql: String)
  extends JDBCPreparedStatement(connection, sql) with CallableStatement {
  private val namedParameters = new util.LinkedHashMap[String, Any]()
  private val outParameterByIndex = TrieMap[Int, Int]()
  private val outParameterByName = TrieMap[String, Int]()

  override def wasNull(): Boolean = false

  override def getString(parameterIndex: Int): String = parameterAs(parameterIndex)

  override def getBoolean(parameterIndex: Int): Boolean = parameterAs(parameterIndex)

  override def getByte(parameterIndex: Int): Byte = parameterAs(parameterIndex)

  override def getShort(parameterIndex: Int): Short = parameterAs(parameterIndex)

  override def getInt(parameterIndex: Int): Int = parameterAs(parameterIndex)

  override def getLong(parameterIndex: Int): Long = parameterAs(parameterIndex)

  override def getFloat(parameterIndex: Int): Float = parameterAs(parameterIndex)

  override def getDouble(parameterIndex: Int): Double = parameterAs(parameterIndex)

  override def getBigDecimal(parameterIndex: Int, scale: Int): java.math.BigDecimal = parameterAs(parameterIndex)

  override def getBytes(parameterIndex: Int): Array[Byte] = parameterAs(parameterIndex)

  override def getDate(parameterIndex: Int): Date = parameterAs(parameterIndex)

  override def getTime(parameterIndex: Int): Time = parameterAs(parameterIndex)

  override def getTimestamp(parameterIndex: Int): Timestamp = parameterAs(parameterIndex)

  override def getObject(parameterIndex: Int): AnyRef = parameterAs(parameterIndex)

  override def getBigDecimal(parameterIndex: Int): java.math.BigDecimal = parameterAs(parameterIndex)

  override def getObject(parameterIndex: Int, map: util.Map[String, Class[_]]): AnyRef = parameterAs(parameterIndex)

  override def getRef(parameterIndex: Int): Ref = parameterAs(parameterIndex)

  override def getBlob(parameterIndex: Int): Blob = parameterAs(parameterIndex)

  override def getClob(parameterIndex: Int): Clob = parameterAs(parameterIndex)

  override def getArray(parameterIndex: Int): SQLArray = parameterAs(parameterIndex)

  override def getDate(parameterIndex: Int, cal: Calendar): Date = parameterAs(parameterIndex)

  override def getTime(parameterIndex: Int, cal: Calendar): Time = parameterAs(parameterIndex)

  override def getTimestamp(parameterIndex: Int, cal: Calendar): Timestamp = parameterAs(parameterIndex)

  override def registerOutParameter(parameterIndex: Int, sqlType: Int): Unit = outParameterByIndex(parameterIndex) = sqlType

  override def registerOutParameter(parameterIndex: Int, sqlType: Int, scale: Int): Unit = outParameterByIndex(parameterIndex) = sqlType

  override def registerOutParameter(parameterIndex: Int, sqlType: Int, typeName: String): Unit = outParameterByIndex(parameterIndex) = sqlType

  override def registerOutParameter(parameterName: String, sqlType: Int): Unit = outParameterByName(parameterName) = sqlType

  override def registerOutParameter(parameterName: String, sqlType: Int, scale: Int): Unit = outParameterByName(parameterName) = sqlType

  override def registerOutParameter(parameterName: String, sqlType: Int, typeName: String): Unit = outParameterByName(parameterName) = sqlType

  override def getURL(parameterIndex: Int): URL = new URL(parameterAs[String](parameterIndex))

  override def setURL(parameterName: String, url: URL): Unit = setParameter(parameterName, url.toExternalForm)

  override def setNull(parameterName: String, sqlType: Int): Unit = setParameter(parameterName, _null(sqlType))

  override def setBoolean(parameterName: String, x: Boolean): Unit = setParameter(parameterName, x)

  override def setByte(parameterName: String, x: Byte): Unit = setParameter(parameterName, x)

  override def setShort(parameterName: String, x: Short): Unit = setParameter(parameterName, x)

  override def setInt(parameterName: String, x: Int): Unit = setParameter(parameterName, x)

  override def setLong(parameterName: String, x: Long): Unit = setParameter(parameterName, x)

  override def setFloat(parameterName: String, x: Float): Unit = setParameter(parameterName, x)

  override def setDouble(parameterName: String, x: Double): Unit = setParameter(parameterName, x)

  override def setBigDecimal(parameterName: String, x: java.math.BigDecimal): Unit = setParameter(parameterName, x)

  override def setString(parameterName: String, x: String): Unit = setParameter(parameterName, x)

  override def setBytes(parameterName: String, x: Array[Byte]): Unit = setParameter(parameterName, x)

  override def setDate(parameterName: String, x: Date): Unit = setParameter(parameterName, x)

  override def setTime(parameterName: String, x: Time): Unit = setParameter(parameterName, x)

  override def setTimestamp(parameterName: String, x: Timestamp): Unit = setParameter(parameterName, x)

  override def setAsciiStream(parameterName: String, x: InputStream, length: Int): Unit = setParameter(parameterName, x.toBlob(length))

  override def setBinaryStream(parameterName: String, x: InputStream, length: Int): Unit = setParameter(parameterName, x.toBlob(length))

  override def setObject(parameterName: String, x: Any, targetSqlType: Int, scale: Int): Unit = setParameter(parameterName, _cast(x, targetSqlType, scale))

  override def setObject(parameterName: String, x: Any, targetSqlType: Int): Unit = setParameter(parameterName, _cast(x, targetSqlType))

  override def setObject(parameterName: String, x: Any): Unit = setParameter(parameterName, x)

  override def setCharacterStream(parameterName: String, reader: Reader, length: Int): Unit = setParameter(parameterName, reader.toBlob(length))

  override def setDate(parameterName: String, x: Date, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setTime(parameterName: String, x: Time, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setTimestamp(parameterName: String, x: Timestamp, cal: Calendar): Unit = setParameter(parameterName, x)

  override def setNull(parameterName: String, sqlType: Int, typeName: String): Unit = setParameter(parameterName, _null(sqlType))

  override def getString(parameterName: String): String = parameterAs(parameterName)

  override def getBoolean(parameterName: String): Boolean = parameterAs(parameterName)

  override def getByte(parameterName: String): Byte = parameterAs(parameterName)

  override def getShort(parameterName: String): Short = parameterAs(parameterName)

  override def getInt(parameterName: String): Int = parameterAs(parameterName)

  override def getLong(parameterName: String): Long = parameterAs(parameterName)

  override def getFloat(parameterName: String): Float = parameterAs(parameterName)

  override def getDouble(parameterName: String): Double = parameterAs(parameterName)

  override def getBytes(parameterName: String): Array[Byte] = parameterAs(parameterName)

  override def getDate(parameterName: String): Date = parameterAs(parameterName)

  override def getTime(parameterName: String): Time = parameterAs(parameterName)

  override def getTimestamp(parameterName: String): Timestamp = parameterAs(parameterName)

  override def getObject(parameterName: String): AnyRef = parameterAs(parameterName)

  override def getBigDecimal(parameterName: String): java.math.BigDecimal = parameterAs(parameterName)

  override def getObject(parameterName: String, map: util.Map[String, Class[_]]): AnyRef = parameterAs(parameterName)

  override def getRef(parameterName: String): Ref = parameterAs(parameterName)

  override def getBlob(parameterName: String): Blob = parameterAs(parameterName)

  override def getClob(parameterName: String): Clob = parameterAs(parameterName)

  override def getArray(parameterName: String): SQLArray = parameterAs(parameterName)

  override def getDate(parameterName: String, cal: Calendar): Date = parameterAs(parameterName)

  override def getTime(parameterName: String, cal: Calendar): Time = parameterAs(parameterName)

  override def getTimestamp(parameterName: String, cal: Calendar): Timestamp = parameterAs(parameterName)

  override def getURL(parameterName: String): URL = parameterAs(parameterName)

  override def getRowId(parameterIndex: Int): RowId = parameterAs(parameterIndex)

  override def getRowId(parameterName: String): RowId = parameterAs(parameterName)

  override def setRowId(parameterName: String, x: RowId): Unit = setParameter(parameterName, x)

  override def setNString(parameterName: String, value: String): Unit = setString(parameterName, value)

  override def setNCharacterStream(parameterName: String, value: Reader, length: Long): Unit = setCharacterStream(parameterName, value, length)

  override def setNClob(parameterName: String, value: NClob): Unit = setClob(parameterName, value)

  override def setClob(parameterName: String, reader: Reader, length: Long): Unit = setParameter(parameterName, reader.toBlob(length))

  override def setBlob(parameterName: String, inputStream: InputStream, length: Long): Unit = setParameter(parameterName, inputStream.toBlob(length))

  override def setNClob(parameterName: String, reader: Reader, length: Long): Unit = setClob(parameterName, reader, length)

  override def getNClob(parameterIndex: Int): NClob = parameterAs(parameterIndex)

  override def getNClob(parameterName: String): NClob = parameterAs(parameterName)

  override def setSQLXML(parameterName: String, xmlObject: SQLXML): Unit = setParameter(parameterName, xmlObject)

  override def getSQLXML(parameterIndex: Int): SQLXML = parameterAs(parameterIndex)

  override def getSQLXML(parameterName: String): SQLXML = parameterAs(parameterName)

  override def getNString(parameterIndex: Int): String = getString(parameterIndex)

  override def getNString(parameterName: String): String = getString(parameterName)

  override def getNCharacterStream(parameterIndex: Int): Reader = getCharacterStream(parameterIndex)

  override def getNCharacterStream(parameterName: String): Reader = getCharacterStream(parameterName)

  override def getCharacterStream(parameterIndex: Int): Reader = getClob(parameterIndex).getCharacterStream

  override def getCharacterStream(parameterName: String): Reader = getClob(parameterName).getCharacterStream

  override def setBlob(parameterName: String, x: Blob): Unit = setParameter(parameterName, x)

  override def setClob(parameterName: String, x: Clob): Unit = setParameter(parameterName, x)

  override def setAsciiStream(parameterName: String, x: InputStream, length: Long): Unit = setParameter(parameterName, x.toBlob(length))

  override def setBinaryStream(parameterName: String, x: InputStream, length: Long): Unit = setParameter(parameterName, x.toBlob(length))

  override def setCharacterStream(parameterName: String, reader: Reader, length: Long): Unit = setParameter(parameterName, reader.toClob(length))

  override def setAsciiStream(parameterName: String, x: InputStream): Unit = setParameter(parameterName, x.toBlob)

  override def setBinaryStream(parameterName: String, x: InputStream): Unit = setParameter(parameterName, x.toBlob)

  override def setCharacterStream(parameterName: String, reader: Reader): Unit = setParameter(parameterName, reader.toClob)

  override def setNCharacterStream(parameterName: String, value: Reader): Unit = setCharacterStream(parameterName, value)

  override def setClob(parameterName: String, reader: Reader): Unit = setParameter(parameterName, reader.toClob)

  override def setBlob(parameterName: String, inputStream: InputStream): Unit = setParameter(parameterName, inputStream.toBlob)

  override def setNClob(parameterName: String, reader: Reader): Unit = setClob(parameterName, reader)

  override def getObject[T](parameterIndex: Int, `type`: Class[T]): T = parameterAs[T](parameterIndex)

  override def getObject[T](parameterName: String, `type`: Class[T]): T = parameterAs[T](parameterName)

  private def setParameter(parameterName: String, value: Any): Unit = namedParameters.put(parameterName, value)

  private def parameterAs[A](parameterIndex: Int): A = parameterMetaData(parameterIndex).asInstanceOf[A]

  private def parameterAs[T](parameterName: String): T = namedParameters.get(parameterName).asInstanceOf[T]

}