package com.qwery.database
package jdbc

import java.io._

import com.qwery.database.types.Clob

/**
 * Character Large Object (CLOB)
 * @param clob the [[RandomAccessFile]]
 */
class JDBCClob(clob: Clob) extends java.sql.Clob with java.sql.NClob {

  override def free(): Unit = clob.free()

  override def getAsciiStream: InputStream = clob.getAsciiStream

  override def setAsciiStream(pos: Long): OutputStream = clob.setAsciiStream(pos)

  override def getCharacterStream: Reader = clob.getCharacterStream

  override def getCharacterStream(pos: Long, length: Long): Reader = clob.getCharacterStream(pos, length)

  override def setCharacterStream(pos: Long): Writer = clob.setCharacterStream(pos)

  override def getSubString(pos: Long, length: Int): String = clob.getSubString(pos, length)

  override def setString(pos: Long, str: String): Int = clob.setString(pos, str)

  override def setString(pos: Long, str: String, offset: Int, len: Int): Int = clob.setString(pos, str, offset, len)

  override def length(): Long = clob.length()

  override def position(searchString: String, start: Long): Long = clob.position(searchString, start)

  override def position(searchString: java.sql.Clob, start: Long): Long = clob.position(searchString, start)

  override def truncate(len: Long): Unit = clob.truncate(len)

}

/**
 * JDBC CLOB Companion
 */
object JDBCClob {

  def create(): JDBCClob = new JDBCClob(Clob.create())
  
  def create(reader: Reader): JDBCClob = new JDBCClob(Clob.create(reader))

  def create(reader: Reader, length: Long): JDBCClob = new JDBCClob(Clob.create(reader, length))

  def load(file: File): JDBCClob = new JDBCClob(Clob.load(file))
  
}