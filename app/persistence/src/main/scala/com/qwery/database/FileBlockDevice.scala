package com.qwery.database

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.BlockDevice.HEADER_CODE

/**
 * File Block Device
 * @param columns         the collection of [[Column columns]]
 * @param persistenceFile the persistence [[File file]]
 */
class FileBlockDevice(val columns: Seq[Column], persistenceFile: File) extends BlockDevice {
  private val raf = new RandomAccessFile(persistenceFile, "rw")

  override def close(): Unit = raf.close()

  override def getPhysicalSize: Option[Long] = Some(raf.length())

  override def length: ROWID = fromOffset(raf.length().toRowID)

  override def readBlock(rowID: ROWID): ByteBuffer = {
    val payload = new Array[Byte](recordSize)
    raf.seek(toOffset(rowID))
    raf.read(payload)
    wrap(payload)
  }

  override def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer = {
    val bytes = new Array[Byte](numberOfBytes)
    raf.seek(toOffset(rowID) + offset)
    raf.read(bytes)
    wrap(bytes)
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    raf.seek(toOffset(rowID))
    RowMetadata.decode(raf.read().toByte)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(toOffset(newSize))
  }

  def verifyHeaderCode(): Unit = {
    // check for the identification code
    raf.seek(0)
    val headerCode = raf.readInt()
    if (headerCode != HEADER_CODE)
      throw new IllegalStateException(f"Device is not a valid block device (header code = $headerCode%08x")
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(metadata.encode)
  }

}
