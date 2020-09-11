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

  override def readBlock(rowID: ROWID): ByteBuffer = {
    if (rowID < capacity) mem.readBlock(rowID) else disk.readBlock(rowID - capacity)
  }

  override def readByte(rowID: ROWID): Byte = {
    if (rowID < capacity) mem.readByte(rowID) else disk.readByte(rowID - capacity)
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): Array[Byte] = {
    if (rowID < capacity) mem.readBytes(rowID, numberOfBytes, offset)
    else disk.readBytes(rowID - capacity, numberOfBytes, offset)
  }

  override def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): PersistentSeq[T] = {
    blocks foreach { case (offset, buf) =>
      writeBlock(offset, buf)
    }
    this
  }

  override def writeByte(rowID: ROWID, byte: ROWID): PersistentSeq[T] = {
    if (rowID < capacity) mem.writeByte(rowID, byte) else disk.writeByte(rowID - capacity, byte)
    this
  }

  override def writeBytes(rowID: ROWID, bytes: Array[Byte]): PersistentSeq[T] = {
    val _p0 = rowID * recordSize
    val _p1 = _p0 + bytes.length

    // should it all land in memory? | p0 < p1 < _capacity |
    if (_p1 <= _capacity) mem.writeBytes(rowID, bytes)

    // should it all land on disk? | _capacity < p0 < p1 |
    else if (_p0 >= _capacity) disk.writeBytes(rowID - capacity, bytes)

    // it's complicated... | p0 < _capacity < p1 | p1 = (offset + length)
    else {
      // compute the distribution
      val (m0, m1) = (rowID, capacity)
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
