package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

/**
 * Byte Array Block Device
 * @param columns  the collection of [[Column columns]]
 * @param capacity the maximum number of item the collection may contain
 */
class ByteArrayBlockDevice(val columns: List[Column], val capacity: Int) extends BlockDevice {
  private val _capacity = capacity * recordSize
  private val array = new Array[Byte](_capacity)
  private var limit: ROWID = 0

  override def close(): Unit = ()

  override def length: ROWID = ((limit / recordSize) + Math.min(1, limit % recordSize)).toRowID

  override def readBlock(rowID: ROWID): ByteBuffer = {
    val p0 = rowID * recordSize
    val bytes = new Array[Byte](recordSize)
    System.arraycopy(array, p0, bytes, 0, bytes.length)
    wrap(bytes)
  }

  override def readByte(rowID: ROWID): Byte = array(rowID * recordSize)

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    val p0 = rowID * recordSize + offset
    val bytes = new Array[Byte](numberOfBytes)
    System.arraycopy(array, p0, bytes, 0, bytes.length)
    wrap(bytes)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < limit) limit = newSize * recordSize
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    val bytes = buf.array()
    System.arraycopy(bytes, 0, array, rowID * recordSize, bytes.length)
    limit = Math.max(limit, rowID * recordSize + 1)
  }

  override def writeByte(rowID: ROWID, byte: Int): Unit = {
    val required = rowID * recordSize + 1
    if (required > _capacity) throw new IllegalStateException(s"Maximum capacity exceeded ($required > $capacity)")
    else {
      array(rowID * recordSize) = byte.toByte
      limit = Math.max(limit, rowID * recordSize + 1)
    }
  }

}
