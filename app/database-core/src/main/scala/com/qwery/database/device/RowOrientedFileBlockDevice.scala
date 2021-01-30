package com.qwery.database.device

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.{BinaryRow, Column, FieldMetadata, ROWID, Row, RowMetadata}

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

/**
 * Row-Oriented File Block Device
 * @param columns         the collection of [[Column columns]]
 * @param file the persistence [[File file]]
 */
class RowOrientedFileBlockDevice(val columns: Seq[Column], file: File) extends RowOrientedBlockDevice {
  private val raf = new RandomAccessFile(file, "rw")

  override def close(): Unit = raf.close()

  override def getPhysicalSize: Option[Long] = Some(raf.length())

  override def length: ROWID = fromOffset(raf.length())

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    val column = columns(columnID)
    val offset = columnOffsets(columnID)
    val bytes = new Array[Byte](column.maxPhysicalSize)
    raf.seek(toOffset(rowID) + offset)
    raf.read(bytes)
    wrap(bytes)
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    raf.seek(toOffset(rowID, columnID))
    FieldMetadata.decode(raf.read().toByte)
  }

  override def readRowAsBinary(rowID: ROWID): ByteBuffer = {
    val payload = new Array[Byte](recordSize)
    raf.seek(toOffset(rowID))
    raf.read(payload)
    wrap(payload)
  }

  override def readRow(rowID: ROWID): BinaryRow = {
    val payload = new Array[Byte](recordSize)
    raf.seek(toOffset(rowID))
    raf.read(payload)
    val buf = wrap(payload)
    BinaryRow(id = rowID, metadata = buf.getRowMetadata, fields = Row.toFieldBuffers(buf)(this))
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    raf.seek(toOffset(rowID))
    RowMetadata.decode(raf.read().toByte)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(toOffset(newSize))
  }

  override def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
  }

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    raf.seek(toOffset(rowID, columnID))
    raf.write(buf.array())
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = {
    raf.seek(toOffset(rowID, columnID))
    raf.write(metadata.encode)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    raf.seek(toOffset(rowID))
    raf.write(metadata.encode)
  }

}
