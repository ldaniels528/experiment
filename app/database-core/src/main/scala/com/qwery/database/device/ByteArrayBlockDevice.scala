package com.qwery.database.device

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.{BinaryRow, Column, FieldMetadata, OffsetOutOfRangeException, ROWID, Row, RowMetadata}

/**
 * Byte Array Block Device (row-oriented)
 * @param columns  the collection of [[Column columns]]
 * @param capacity the maximum number of item the collection may contain
 */
class ByteArrayBlockDevice(val columns: Seq[Column], val capacity: Int) extends RowOrientedBlockDevice {
  private val _capacity = toOffset(capacity)
  private val array = new Array[Byte](_capacity)
  private var limit: ROWID = 0

  override def close(): Unit = ()

  override def getPhysicalSize: Option[Long] = Some(limit)

  override def length: ROWID = fromOffset(limit)

  override def readRowAsBinary(rowID: ROWID): ByteBuffer = {
    val p0 = toOffset(rowID)
    val bytes = new Array[Byte](recordSize)
    System.arraycopy(array, p0, bytes, 0, bytes.length)
    wrap(bytes)
  }

  override def readRow(rowID: ROWID): BinaryRow = {
    val p0 = toOffset(rowID)
    val bytes = new Array[Byte](recordSize)
    System.arraycopy(array, p0, bytes, 0, bytes.length)
    val buf = wrap(bytes)
    BinaryRow(id = rowID, metadata = buf.getRowMetadata, fields = Row.toFieldBuffers(buf)(this))
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    FieldMetadata.decode(array(toOffset(rowID, columnID)))
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = RowMetadata.decode(array(toOffset(rowID)))

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    val column = columns(columnID)
    val p0 = toOffset(rowID, columnID)
    val bytes = new Array[Byte](column.maxPhysicalSize)
    System.arraycopy(array, p0, bytes, 0, bytes.length)
    wrap(bytes)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize >= 0 && newSize < limit) limit = toOffset(newSize)
  }

  override def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit = {
    val offset = toOffset(rowID)
    val required = offset + 1
    assert(required <= _capacity, throw OffsetOutOfRangeException(required, capacity))
    val bytes = buf.array()
    System.arraycopy(bytes, 0, array, offset, bytes.length)
    limit = Math.max(limit, required)
  }

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    val offset = toOffset(rowID, columnID)
    val required = offset + 1
    assert(required <= _capacity, throw OffsetOutOfRangeException(required, capacity))
    val bytes = buf.array()
    System.arraycopy(bytes, 0, array, offset, bytes.length)
    limit = Math.max(limit, required)
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = {
    val offset = toOffset(rowID, columnID)
    val required = offset + 1
    assert(required <= _capacity, throw OffsetOutOfRangeException(required, capacity))
    array(offset) = metadata.encode
    limit = Math.max(limit, required)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    val offset = toOffset(rowID)
    val required = offset + 1
    assert(required <= _capacity, throw OffsetOutOfRangeException(required, capacity))
    array(offset) = metadata.encode
    limit = Math.max(limit, required)
  }

}
