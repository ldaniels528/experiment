package com.qwery.database

import java.nio.ByteBuffer

/**
 * Hybrid Block Device
 * @param columns  the collection of [[Column columns]]
 * @param capacity the maximum number of item the collection may contain
 * @param disk     the overflow [[BlockDevice device]]
 */
class HybridBlockDevice(val columns: List[Column], val capacity: Int, disk: BlockDevice) extends BlockDevice {
  private val mem = new ByteArrayBlockDevice(columns, capacity)

  override def close(): Unit = {
    mem.close()
    disk.close()
  }

  override def length: ROWID = mem.length + disk.length

  override def readBlock(rowID: ROWID): ByteBuffer = {
    if (rowID < capacity) mem.readBlock(rowID) else disk.readBlock(rowID - capacity)
  }

  override def readByte(rowID: ROWID): Byte = {
    if (rowID < capacity) mem.readByte(rowID) else disk.readByte(rowID - capacity)
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    if (rowID < capacity) mem.readBytes(rowID, numberOfBytes, offset) else disk.readBytes(rowID - capacity, numberOfBytes, offset)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize < capacity) {
      mem.shrinkTo(newSize)
      disk.shrinkTo(0)
    }
    else disk.shrinkTo(newSize - capacity)
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    if (rowID < capacity) mem.writeBlock(rowID, buf) else disk.writeBlock(rowID - capacity, buf)
  }

  override def writeByte(rowID: ROWID, byte: ROWID): Unit = {
    if (rowID < capacity) mem.writeByte(rowID, byte) else disk.writeByte(rowID - capacity, byte)
  }

}
