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

  override def readHeader: BlockDevice.Header = FileBlockDevice.getHeader(raf)

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    raf.seek(toOffset(rowID))
    RowMetadata.decode(raf.read().toByte)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(toOffset(newSize))
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
  }

  override def writeHeader(header: BlockDevice.Header): Unit = {
    raf.seek(0)
    raf.write(header.toBuffer.array())
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(metadata.encode)
  }

}

/**
 * File Block Device Companion
 */
object FileBlockDevice {

  def getHeader(file: File): BlockDevice.Header = {
    val raf = new RandomAccessFile(file, "r")
    try getHeader(raf) finally raf.close()
  }

  def getHeader(raf: RandomAccessFile): BlockDevice.Header = {
    if (raf.length() < INT_BYTES + INT_BYTES)
      throw new IllegalStateException(s"Device is not a valid block device (length = ${raf.length()})")
    else {
      // check for the identification code
      raf.seek(0)
      val headerCode = raf.readInt()
      if (headerCode != HEADER_CODE)
        throw new IllegalStateException(f"Device is not a valid block device (header code = $headerCode%08x")
      else {
        // read the header length
        raf.seek(INT_BYTES)
        val headerSize = raf.readInt()

        // read the header bytes
        raf.seek(INT_BYTES + INT_BYTES)
        val headerBytes = new Array[Byte](headerSize)
        raf.read(headerBytes)

        // return the header
        BlockDevice.Header.fromBuffer(wrap(headerBytes))
      }
    }
  }

}