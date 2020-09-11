package com.qwery.database

import java.nio.ByteBuffer

import scala.reflect.ClassTag

/**
 * Hybrid Persistent Sequence
 * @param capacity the internal memory capacity
 * @tparam T the product type
 */
class HybridPersistentSeq[T <: Product : ClassTag](capacity: Int = 50000) extends PersistentSeq[T]() {
  private val disk = new DiskMappedSeq[T]()
  private val mem = new MemoryMappedSeq[T](capacity)
  private val _capacity = capacity * recordSize

  override def close(): Unit = {
    mem.close()
    disk.close()
  }

  override def length: ROWID = mem.length + disk.length

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize < capacity) {
      mem.shrinkTo(newSize)
      disk.shrinkTo(0)
    }
    else disk.shrinkTo(newSize - capacity)
  }

  override def readBlock(rowID: ROWID): ByteBuffer = {
    if (rowID < capacity) mem.readBlock(rowID) else disk.readBlock(rowID - capacity)
  }

  override def readByte(rowID: ROWID): Byte = {
    if (rowID < capacity) mem.readByte(rowID) else disk.readByte(rowID - capacity)
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    if (rowID < capacity) mem.readBytes(rowID, numberOfBytes, offset) else disk.readBytes(rowID - capacity, numberOfBytes, offset)
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    if (rowID < capacity) mem.writeBlock(rowID, buf) else disk.writeBlock(rowID - capacity, buf)
  }

  override def writeByte(rowID: ROWID, byte: ROWID): Unit = {
    if (rowID < capacity) mem.writeByte(rowID, byte) else disk.writeByte(rowID - capacity, byte)
  }

}
