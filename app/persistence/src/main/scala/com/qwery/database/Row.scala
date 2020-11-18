package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.device.BlockDevice
import com.qwery.database.types.{QxAny, QxInt}

/**
 * Represents a database row
 * @param id       the row's unique [[ROWID row identifier]]
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(id: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  /**
   * Returns the referenced row ID (if read from an index)
   * @return an option of the referenced row ID
   */
  def getReferencedRowID: Option[ROWID] = {
    fields.collectFirst { case Field(name, _, QxInt(Some(rowID))) if name == ROWID_NAME => rowID: ROWID }
  }

  /**
   * @param device the implicit [[BlockDevice]]
   * @return a [[BinaryRow]] representation of the row
   */
  def toBinaryRow(implicit device: BlockDevice): BinaryRow = {
    BinaryRow(id, metadata, fields = Row.toFieldBuffers(fields))
  }

  /**
   * @return a [[Map]] representation of the row
   */
  def toMap: Map[String, Any] = Map((ROWID_NAME -> id) :: fields.flatMap(f => f.value.map(f.name -> _)).toList: _*)

  def toRowBuffer(implicit device: BlockDevice): ByteBuffer = {
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(metadata)
    Row.toFieldBuffers(fields) zip device.columnOffsets foreach { case (fieldBuf, offset) =>
      buf.position(offset)
      buf.put(fieldBuf)
    }
    buf.flip()
    buf
  }

  /**
   * @return a [[RowTuple]] representation of the row
   */
  def toRowTuple: RowTuple = RowTuple(toMap)

}

/**
 * Row Companion
 */
object Row {

  def toFieldBuffers(buf: ByteBuffer)(implicit device: BlockDevice): Seq[ByteBuffer] = {
    device.physicalColumns.zipWithIndex map { case (column, index) =>
      buf.position(device.columnOffsets(index))
      val fieldBytes = new Array[Byte](column.maxPhysicalSize)
      buf.get(fieldBytes)
      wrap(fieldBytes)
    }
  }

  def toFieldBuffers(fields: Seq[Field])(implicit device: BlockDevice): Seq[ByteBuffer] = {
    device.columns zip fields map { case (column, field) =>
      val buf = allocate(column.maxPhysicalSize)
      val bytes = Codec.encode(column, field.typedValue.value)
      buf.put(bytes)
      buf.flip()
      buf
    }
  }

  def toFields(buf: ByteBuffer)(implicit device: BlockDevice): Seq[Field] = {
    device.physicalColumns.zipWithIndex map { case (column, index) =>
      buf.position(device.columnOffsets(index))
      column.name -> QxAny.decode(column, buf)
    } map { case (name, (fmd, value_?)) => Field(name, fmd, value_?) }
  }

}