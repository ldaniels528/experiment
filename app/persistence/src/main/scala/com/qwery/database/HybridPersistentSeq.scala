package com.qwery.database

import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Hybrid Persistent Sequence
 * @param capacity the internal memory capacity
 * @tparam T the product type
 */
class HybridPersistentSeq[T <: Product : ClassTag](capacity: Int = 50000) extends PersistentSeq[T]() {
  private val emptyArray = new Array[Byte](0)
  private val disk = new DiskMappedSeq[T]()
  private val mem = new MemoryMappedSeq[T](capacity)
  private val _capacity = capacity * recordSize

  override def close(): Unit = {
    mem.close()
    disk.close()
  }

  override def length: ROWID = mem.length + disk.length

  override def shrinkTo(newSize: ROWID): PersistentSeq[T] = {
    if (newSize < capacity) {
      mem.shrinkTo(newSize)
      disk.shrinkTo(0)
    }
    else disk.shrinkTo(newSize - capacity)
    this
  }

  override protected def newDocument[A <: Product : ClassTag](): PersistentSeq[A] = new HybridPersistentSeq[A](capacity)

  override def readBlock(offset: ROWID): ByteBuffer = {
    if (offset < capacity) mem.readBlock(offset) else disk.readBlock(offset - capacity)
  }

  override def readByte(offset: ROWID): Byte = {
    if (offset < capacity) mem.readByte(offset) else disk.readByte(offset - capacity)
  }

  override def readBytes(offset: ROWID, numberOfBlocks: ROWID): Array[Byte] = {
    val _p0 = offset * recordSize
    val _p1 = _p0 + numberOfBlocks * recordSize

    // should it all come from memory? | p0 < p1 < _capacity |
    if (_p1 <= _capacity) mem.readBytes(offset, numberOfBlocks)

    // should it all come from disk? | _capacity < p0 < p1 |
    else if (_p0 >= _capacity) disk.readBytes(offset - capacity, numberOfBlocks)

    // it's complicated... | p0 < _capacity < p1 |
    else {
      // compute the distribution
      val (m0, m1) = (offset, capacity)
      val (d0, d1) = (capacity, m0 + numberOfBlocks)
      val (mLen, dLen) = (m1 - m0, d1 - d0)

      // copy the data
      val buf0 = if (mLen > 0) mem.readBytes(m0, mLen) else emptyArray
      val buf1 = if (dLen > 0) disk.readBytes(d0 - capacity, dLen) else emptyArray
      ByteBuffer.allocate(buf0.length + buf1.length).put(buf0).put(buf1).array()
    }
  }

  override def readFragment(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): Array[Byte] = {
    if (rowID < capacity) mem.readFragment(rowID, numberOfBytes, offset)
    else disk.readFragment(rowID - capacity, numberOfBytes, offset)
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): PersistentSeq[T] = {
    blocks foreach { case (offset, buf) =>
      writeBlock(offset, buf)
    }
    this
  }

  override def writeByte(offset: ROWID, byte: ROWID): PersistentSeq[T] = {
    if (offset < capacity) mem.writeByte(offset, byte) else disk.writeByte(offset - capacity, byte)
    this
  }

  override def writeBytes(offset: ROWID, bytes: Array[Byte]): PersistentSeq[T] = {
    val _p0 = offset * recordSize
    val _p1 = _p0 + bytes.length

    // should it all land in memory? | p0 < p1 < _capacity |
    if (_p1 <= _capacity) mem.writeBytes(offset, bytes)

    // should it all land on disk? | _capacity < p0 < p1 |
    else if (_p0 >= _capacity) disk.writeBytes(offset - capacity, bytes)

    // it's complicated... | p0 < _capacity < p1 | p1 = (offset + length)
    else {
      // compute the distribution
      val (m0, m1) = (offset, capacity)
      val (d0, d1) = (capacity, m0 + (bytes.length / recordSize)) // + Math.min(bytes.length % recordSize, 1)
      val (mLen, dLen) = (m1 - m0, d1 - d0)
      val (_m0, _m1) = (m0 * recordSize, m1 * recordSize)
      val (_d0, _d1) = (d0 * recordSize, _m0 + bytes.length)
      val (_mLen, _dLen) = (_m1 - _m0, _d1 - _d0)

      // create a temporary buffer for copying/writing the data
      val scratch = new Array[Byte](Math.max(_mLen, _dLen))

      // copy the lower region to disk
      if (dLen > 0) {
        val dOffset = d0 - capacity
        System.arraycopy(bytes, _d0, scratch, 0, _dLen)
        disk.writeBytes(dOffset, scratch)
      }

      // copy the upper region to memory
      if (mLen > 0) {
        System.arraycopy(bytes, _m0, scratch, 0, _mLen)
        mem.writeBytes(m0, scratch)
      }
    }
    this
  }

}
