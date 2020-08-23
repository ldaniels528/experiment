package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Hybrid Persistent Sequence
 * @param capacity the internal memory capacity
 * @tparam T the product type
 */
class HybridPersistentSeq[T <: Product : ClassTag](capacity: Int = 50000) extends PersistentSeq[T]() {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val disk = new DiskMappedSeq[T]()
  private val mem = new MemoryMappedSeq[T](capacity)
  private val _capacity = capacity * recordSize

  override def close(): Unit = {
    mem.close()
    disk.close()
  }

  override def length: URID = mem.length + disk.length

  override def shrinkTo(newSize: URID): PersistentSeq[T] = {
    if (newSize < capacity) {
      mem.shrinkTo(newSize)
      disk.shrinkTo(0)
    }
    else disk.shrinkTo(newSize - capacity)
    this
  }

  override protected def newDocument[A <: Product : ClassTag](): PersistentSeq[A] = new HybridPersistentSeq[A](capacity)

  override def readBlocks(offset: URID, numberOfBlocks: URID): ByteBuffer = {
    val _p0 = offset * recordSize
    val _p1 = _p0 + numberOfBlocks * recordSize

    // should it all come from memory? | p0 < p1 < _capacity |
    if (_p1 <= _capacity) mem.readBlocks(offset, numberOfBlocks)

    // should it all come from disk? | _capacity < p0 < p1 |
    else if (_p0 >= _capacity) disk.readBlocks(offset - capacity, numberOfBlocks)

    // it's complicated... | p0 < _capacity < p1 |
    else {
      val m0 = offset
      val m1 = capacity
      val d0 = capacity
      val d1 = m0 + numberOfBlocks
      val (mLen, dLen) = (m1 - m0, d1 - d0)

      val buf0 = if (mLen > 0) mem.readBlocks(m0, mLen) else wrap(new Array[Byte](0))
      val buf1 = if (dLen > 0) disk.readBlocks(d0 - capacity, dLen) else wrap(new Array[Byte](0))
      ByteBuffer.allocate(buf0.capacity() + buf1.capacity()).put(buf0).put(buf1)
    }
  }

  override def writeBlocks(offset: URID, bytes: Array[Byte]): PersistentSeq[T] = {
    val _p0 = offset * recordSize
    val _p1 = _p0 + bytes.length

    // should it all land in memory? | p0 < p1 < _capacity |
    if (_p1 <= _capacity) mem.writeBlocks(offset, bytes)

    // should it all land on disk? | _capacity < p0 < p1 |
    else if (_p0 >= _capacity) disk.writeBlocks(offset - capacity, bytes)

    // it's complicated... | p0 < _capacity < p1 | p1 = (offset + length)
    else {
      val m0 = offset
      val m1 = capacity
      val d0 = capacity
      val d1 = m0 + (bytes.length / recordSize)// + Math.min(bytes.length % recordSize, 1)
      val (mLen, dLen) = (m1 - m0, d1 - d0)

      val _m0 = m0 * recordSize
      val _m1 = m1 * recordSize
      val _d0 = d0 * recordSize
      val _d1 = _m0 + bytes.length
      val (_mLen, _dLen) = (_m1 - _m0, _d1 - _d0)

      val scratch = new Array[Byte](Math.max(_mLen, _dLen))

      if (dLen > 0) {
        val dOffset = d0 - capacity
        System.arraycopy(bytes, _d0, scratch, 0, _dLen)
        disk.writeBlocks(dOffset, scratch)
      }

      if (mLen > 0) {
        System.arraycopy(bytes, _m0, scratch, 0, _mLen)
        mem.writeBlocks(m0, scratch)
      }
    }
    this
  }

  override def readByte(offset: URID): Byte = {
    if (offset < capacity) mem.readByte(offset) else disk.readByte(offset - capacity)
  }

  override def writeByte(offset: URID, byte: URID): PersistentSeq[T] = {
    if (offset < capacity) mem.writeByte(offset, byte) else disk.writeByte(offset - capacity, byte)
    this
  }

}
