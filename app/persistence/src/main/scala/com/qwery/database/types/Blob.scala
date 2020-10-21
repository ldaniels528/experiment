package com.qwery.database.types

import java.io.{File, InputStream, OutputStream, RandomAccessFile}

import com.qwery.database.PersistentSeq.newTempFile
import org.apache.commons.io.IOUtils

/**
 * Binary Large Object (BLOB)
 * @param raf the [[RandomAccessFile]]
 */
class Blob(raf: RandomAccessFile) {
  private var position: Long = 0L

  def free(): Unit = raf.close()

  def getInputStream: InputStream = () => {
    val byte = raf.read()
    position += 1
    byte
  }

  def getInputStream(pos: Long, length: Long): InputStream = new InputStream {
    position = pos
    override def read(): Int = {
      raf.seek(pos)
      val byte = raf.read()
      position += 1
      byte
    }
  }

  def getOutputStream(pos: Long): OutputStream = new OutputStream {
    position = pos
    override def write(byte: Int): Unit = {
      raf.seek(pos)
      raf.write(byte)
      position = pos + 1
    }
  }

  def getBytes(pos: Long, length: Int): Array[Byte] = {
    val bytes = new Array[Byte](length)
    raf.read(bytes, pos.toInt, length)
    position = pos + length
    bytes
  }

  def setBytes(pos: Long, bytes: Array[Byte]): Int = {
    raf.seek(pos)
    raf.write(bytes)
    position = pos + bytes.length
    bytes.length
  }

  def setBytes(pos: Long, bytes: Array[Byte], offset: Int, len: Int): Int = {
    raf.seek(pos)
    raf.write(bytes, offset, len)
    position = pos + len
    len
  }

  def length(): Long = raf.length()

  def position(pattern: Array[Byte], start: Long): Long = ??? // TODO do something about this some day

  def position(pattern: java.sql.Blob, start: Long): Long = ??? // TODO do something about this some day

  def truncate(len: Long): Unit = {
    raf.setLength(len)
    position = position min len
  }

}

/**
 * BLOB Companion
 */
object Blob {

  def create(): Blob = new Blob(new RandomAccessFile(newTempFile(), "rw"))

  def create(inputStream: InputStream): Blob = {
    val blob = new Blob(new RandomAccessFile(newTempFile(), "rw"))
    IOUtils.copy(inputStream, blob.getOutputStream(0))
    blob
  }

  def create(inputStream: InputStream, length: Long): Blob = {
    val blob = new Blob(new RandomAccessFile(newTempFile(), "rw"))
    IOUtils.copy(inputStream, blob.getOutputStream(0))
    blob.truncate(length)
    blob
  }

  def load(file: File): Blob = new Blob(new RandomAccessFile(file, "rw"))

}