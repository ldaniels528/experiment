package com.qwery.database
package jdbc

import java.io.{File, InputStream, OutputStream}

import com.qwery.database.types.Blob

/**
 * Binary Large Object (BLOB)
 * @param blob the [[Blob]]
 */
class JDBCBlob(blob: Blob) extends java.sql.Blob {

  override def free(): Unit = blob.free()

  override def getBinaryStream: InputStream = blob.getInputStream

  override def getBinaryStream(pos: Long, length: Long): InputStream = blob.getInputStream(pos, length)

  override def setBinaryStream(pos: Long): OutputStream = blob.getOutputStream(pos)

  override def getBytes(pos: Long, length: Int): Array[Byte] = blob.getBytes(pos, length)

  override def setBytes(pos: Long, bytes: Array[Byte]): Int = blob.setBytes(pos, bytes)

  override def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = blob.setBytes(pos, bytes)

  override def length(): Long = blob.length()

  override def position(pattern: Array[Byte], start: Long): Long = blob.position(pattern, start)

  override def position(pattern: java.sql.Blob, start: Long): Long = blob.position(pattern, start)

  override def truncate(len: Long): Unit = blob.truncate(len)

}

/**
 * JDBC BLOB Companion
 */
object JDBCBlob {

  def create(): JDBCBlob = new JDBCBlob(Blob.create())

  def create(inputStream: InputStream): JDBCBlob = new JDBCBlob(Blob.create(inputStream))

  def create(inputStream: InputStream, length: Long): JDBCBlob = new JDBCBlob(Blob.create(inputStream, length))

  def load(file: File): JDBCBlob = new JDBCBlob(Blob.load(file))

}
