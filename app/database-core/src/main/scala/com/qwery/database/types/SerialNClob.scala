package com.qwery.database.types

import java.io.{InputStream, OutputStream, Reader, Writer}
import java.sql.{Clob, NClob}

import javax.sql.rowset.serial.SerialClob

class SerialNClob(chars: Array[Char]) extends NClob {
  private val clob = new SerialClob(chars)

  override def length(): Long = clob.length()

  override def getSubString(pos: Long, length: Int): String = clob.getSubString(pos, length)

  override def getCharacterStream: Reader = clob.getCharacterStream

  override def getAsciiStream: InputStream = clob.getAsciiStream

  override def position(searchstr: String, start: Long): Long = clob.position(searchstr, start)

  override def position(searchstr: Clob, start: Long): Long = clob.position(searchstr, start)

  override def setString(pos: Long, str: String): Int = clob.setString(pos, str)

  override def setString(pos: Long, str: String, offset: Int, len: Int): Int = clob.setString(pos, str, offset, len)

  override def setAsciiStream(pos: Long): OutputStream = clob.setAsciiStream(pos)

  override def setCharacterStream(pos: Long): Writer = clob.setCharacterStream(pos)

  override def truncate(len: Long): Unit = clob.truncate(len)

  override def free(): Unit = clob.free()

  override def getCharacterStream(pos: Long, length: Long): Reader = clob.getCharacterStream(pos, length)
}
