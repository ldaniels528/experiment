package com.qwery.database

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.DiskMappedSeq.newTempFile

import scala.reflect.ClassTag

/**
 * Represents a random-access disk-mapped collection implementation
 * @param persistenceFile the [[File persistence file]]
 */
class DiskMappedSeq[T <: Product : ClassTag](val persistenceFile: File) extends PersistentSeq[T] {
  private val raf = new RandomAccessFile(persistenceFile, "rw")

  /**
   * Default constructor
   */
  def this() = this(newTempFile())

  override def close(): Unit = raf.close()

  override def length: ROWID = {
    val eof = raf.length()
    ((eof / recordSize) + Math.min(1, eof % recordSize)).toURID
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(newSize * recordSize)
  }

  override def readBlock(rowID: ROWID): ByteBuffer = {
    val payload = new Array[Byte](recordSize)
    raf.seek(rowID * recordSize)
    raf.read(payload)
    wrap(payload)
  }

  override def readByte(rowID: ROWID): Byte = {
    raf.seek(rowID * recordSize)
    raf.read().toByte
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    val bytes = new Array[Byte](numberOfBytes)
    raf.seek(rowID * recordSize + offset)
    raf.read(bytes)
    wrap(bytes)
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    raf.seek(rowID * recordSize)
    raf.write(buf.array())
  }

  override def writeByte(rowID: ROWID, byte: Int): Unit = {
    raf.seek(rowID * recordSize)
    raf.write(byte)
  }

}

/**
 * RandomAccessFileSeq Companion
 */
object DiskMappedSeq {

  def apply[T <: Product : ClassTag](): DiskMappedSeq[T] = new DiskMappedSeq[T]()

  def newTempFile(): File = {
    val file = File.createTempFile("persistent", ".lldb")
    file.deleteOnExit()
    file
  }

}