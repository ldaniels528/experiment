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

  override def length: URID = {
    val eof = raf.length()
    ((eof / recordSize) + Math.min(1, eof % recordSize)).toURID
  }

  override def shrinkTo(newSize: URID): PersistentSeq[T] = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(newSize * recordSize)
    this
  }

  def readBlocks(offset: URID, numberOfBlocks: Int = 1): ByteBuffer = {
    val payload = new Array[Byte](recordSize * numberOfBlocks)
    raf.seek(offset * recordSize)
    raf.read(payload)
    wrap(payload)
  }

  def writeBlocks(offset: URID, bytes: Array[Byte]): PersistentSeq[T] = {
    raf.seek(offset * recordSize)
    raf.write(bytes)
    this
  }

  def readByte(offset: URID): Byte = {
    raf.seek(offset * recordSize)
    raf.read().toByte
  }

  def writeByte(offset: URID, byte: Int): PersistentSeq[T] = {
    raf.seek(offset * recordSize)
    raf.write(byte)
    this
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