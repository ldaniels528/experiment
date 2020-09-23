package com.qwery.database

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

/**
 * File Block Device
 * @param columns         the collection of [[Column columns]]
 * @param persistenceFile the persistence [[File file]]
 */
class FileBlockDevice(val columns: List[Column], persistenceFile: File) extends BlockDevice {
  private val raf = new RandomAccessFile(persistenceFile, "rw")

  override def close(): Unit = raf.close()

  override def length: ROWID = {
    val eof = raf.length()
    ((eof / recordSize) + Math.min(1, eof % recordSize)).toRowID
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

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(newSize * recordSize)
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
