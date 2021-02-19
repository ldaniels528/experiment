package com.qwery.database.device

import com.qwery.database.models.{BinaryRow, Column, FieldMetadata, RowMetadata}

import java.nio.ByteBuffer
import com.qwery.database.ROWID

/**
 * Hybrid Block Device
 * @param columns  the collection of [[Column columns]]
 * @param capacity the maximum number of item the collection may contain
 * @param disk     the overflow [[BlockDevice device]]
 */
class HybridBlockDevice(val columns: Seq[Column], val capacity: Int, disk: BlockDevice) extends BlockDevice {
  private val mem = new ByteArrayBlockDevice(columns, capacity)

  override def close(): Unit = {
    mem.close()
    disk.close()
  }

  override def getPhysicalSize: Option[Long] = for {sizeA <- mem.getPhysicalSize; sizeB <- disk.getPhysicalSize} yield sizeA + sizeB

  override def length: ROWID = mem.length + disk.length

  override def readRowAsBinary(rowID: ROWID): ByteBuffer = {
    if (rowID < capacity) mem.readRowAsBinary(rowID) else disk.readRowAsBinary(rowID - capacity)
  }

  override def readField(rowID: ROWID, columnID: Int): ByteBuffer = {
    if (rowID < capacity) mem.readField(rowID, columnID) else disk.readField(rowID - capacity, columnID)
  }

  override def readFieldMetaData(rowID: ROWID, columnID: Int): FieldMetadata = {
    if (rowID < capacity) mem.readFieldMetaData(rowID, columnID) else disk.readFieldMetaData(rowID - capacity, columnID)
  }

  override def readRow(rowID: ROWID): BinaryRow = {
    if (rowID < capacity) mem.readRow(rowID) else disk.readRow(rowID - capacity)
  }

  override def readRowMetaData(rowID: ROWID): RowMetadata = {
    if (rowID < capacity) mem.readRowMetaData(rowID) else disk.readRowMetaData(rowID - capacity)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    if (newSize < capacity) {
      mem.shrinkTo(newSize)
      disk.shrinkTo(0)
    }
    else disk.shrinkTo(newSize - capacity)
  }

  override def writeRowAsBinary(rowID: ROWID, buf: ByteBuffer): Unit = {
    if (rowID < capacity) mem.writeRowAsBinary(rowID, buf) else disk.writeRowAsBinary(rowID - capacity, buf)
  }

  override def writeField(rowID: ROWID, columnID: Int, buf: ByteBuffer): Unit = {
    if (rowID < capacity) mem.writeField(rowID, columnID, buf) else disk.writeField(rowID - capacity, columnID, buf)
  }

  override def writeFieldMetaData(rowID: ROWID, columnID: Int, metadata: FieldMetadata): Unit = {
    if (rowID < capacity) mem.writeFieldMetaData(rowID, columnID, metadata) else disk.writeFieldMetaData(rowID - capacity, columnID, metadata)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    if (rowID < capacity) mem.writeRowMetaData(rowID, metadata) else disk.writeRowMetaData(rowID - capacity, metadata)
  }

}
