package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import scala.reflect.ClassTag

/**
 * Represents a memory-mapped collection implementation
 * @param capacity the collection's storage capacity
 */
class MemoryMappedSeq[T <: Product : ClassTag](val capacity: Int) extends PersistentSeq[T] {
  private val _capacity = capacity * recordSize
  private val raf = new Array[Byte](_capacity)
  private var limit: URID = 0

  override def close(): Unit = ()

  override def length: URID = ((limit / recordSize) + Math.min(1, limit % recordSize)).toURID

  override protected def newDocument[A <: Product : ClassTag](): PersistentSeq[A] = new MemoryMappedSeq[A](capacity)

  override def shrinkTo(newSize: URID): PersistentSeq[T] = {
    if (newSize >= 0 && newSize < limit) limit = newSize * recordSize
    this
  }

  override def readBlocks(offset: URID, numberOfBlocks: URID): ByteBuffer = {
    val p0 = offset * recordSize
    val bytes = new Array[Byte](numberOfBlocks * recordSize)
    System.arraycopy(raf, p0, bytes, 0,  bytes.length)
    wrap(bytes)
  }

  override def writeBlocks(offset: URID, bytes: Array[Byte]): PersistentSeq[T] = {
    val required = offset * recordSize + bytes.length
    if (required > _capacity) throw new IllegalStateException(s"Maximum capacity exceeded ($required > ${_capacity}) [capacity: $capacity, recordSize: $recordSize]")
    else {
      System.arraycopy(bytes, 0, raf, offset * recordSize, bytes.length)
      limit = Math.max(limit, required)
      this
    }
  }

  override def readByte(offset: URID): Byte = raf(offset * recordSize)

  override def writeByte(offset: URID, byte: Int): PersistentSeq[T] = {
    val required = offset * recordSize + 1
    if (required > _capacity) throw new IllegalStateException(s"Maximum capacity exceeded ($required > $capacity)")
    else {
      raf(offset * recordSize) = byte.toByte
      limit = Math.max(limit, offset * recordSize + 1)
      this
    }
  }

}
