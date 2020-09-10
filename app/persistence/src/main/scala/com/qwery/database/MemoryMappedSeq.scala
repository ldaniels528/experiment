package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import scala.reflect.ClassTag

/**
 * Represents a memory-mapped collection implementation
 * @param capacity the maximum number of item the collection may contain
 */
class MemoryMappedSeq[T <: Product : ClassTag](val capacity: Int) extends PersistentSeq[T] {
  private val _capacity = capacity * recordSize
  private val raf = new Array[Byte](_capacity)
  private var limit: ROWID = 0

  override def close(): Unit = ()

  override def length: ROWID = ((limit / recordSize) + Math.min(1, limit % recordSize)).toURID

  override protected def newDocument[A <: Product : ClassTag](): PersistentSeq[A] = new MemoryMappedSeq[A](capacity)

  override def shrinkTo(newSize: ROWID): PersistentSeq[T] = {
    if (newSize >= 0 && newSize < limit) limit = newSize * recordSize
    this
  }

  override def readBlock(rowID: ROWID): ByteBuffer = {
    val p0 = rowID * recordSize
    val bytes = new Array[Byte](recordSize)
    System.arraycopy(raf, p0, bytes, 0,  bytes.length)
    wrap(bytes)
  }

  override def readByte(rowID: ROWID): Byte = raf(rowID * recordSize)

  override def readBytes(rowID: ROWID, numberOfBlocks: ROWID): Array[Byte] = {
    val p0 = rowID * recordSize
    val bytes = new Array[Byte](numberOfBlocks * recordSize)
    System.arraycopy(raf, p0, bytes, 0,  bytes.length)
    bytes
  }

  override def readFragment(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): Array[Byte] = {
    val p0 = rowID * recordSize + offset
    val bytes = new Array[Byte](numberOfBytes)
    System.arraycopy(raf, p0, bytes, 0,  bytes.length)
    bytes
  }

  override def writeBytes(rowID: ROWID, bytes: Array[Byte]): PersistentSeq[T] = {
    val required = rowID * recordSize + bytes.length
    if (required > _capacity) throw new IllegalStateException(s"Maximum capacity exceeded ($required > ${_capacity}) [capacity: $capacity, recordSize: $recordSize]")
    else {
      System.arraycopy(bytes, 0, raf, rowID * recordSize, bytes.length)
      limit = Math.max(limit, required)
      this
    }
  }

  override def writeByte(rowID: ROWID, byte: Int): PersistentSeq[T] = {
    val required = rowID * recordSize + 1
    if (required > _capacity) throw new IllegalStateException(s"Maximum capacity exceeded ($required > $capacity)")
    else {
      raf(rowID * recordSize) = byte.toByte
      limit = Math.max(limit, rowID * recordSize + 1)
      this
    }
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): PersistentSeq[T] = {
    blocks foreach { case (offset, buf) =>
      writeBlock(offset, buf)
    }
    this
  }

}
