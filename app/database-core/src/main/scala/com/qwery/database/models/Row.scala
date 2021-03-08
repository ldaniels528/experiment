package com.qwery.database
package models

import com.qwery.database.device.BlockDevice
import com.qwery.database.types.{QxAny, QxInt}
import com.qwery.database.util.Codec
import com.qwery.database.util.Codec.CodecByteBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

/**
 * Represents a database row
 * @param id       the row's unique [[ROWID row identifier]]
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(id: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  /**
   * Retrieves a field by column ID
   * @param columnID the column ID
   * @return the [[Field]]
   */
  def getField(columnID: Int): Field = fields(columnID)

  /**
   * Retrieves a field by column name
   * @param name the name of the field
   * @return the option of a [[Field]]
   */
  def getField(name: String): Option[Field] = fields.find(_.name == name)

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
    models.BinaryRow(id, metadata, fields = Row.toFieldBuffers(fields))
  }

  /**
    * @param device the implicit [[BlockDevice]]
    * @return a [[BinaryRow]] representation of the row
    */
  def toBinaryRow(id: ROWID)(implicit device: BlockDevice): BinaryRow = {
    models.BinaryRow(id, metadata, fields = Row.toFieldBuffers(fields))
  }

  /**
   * @return a [[KeyValues key-value pairs representation]] of the row
   */
  def toKeyValues: KeyValues = KeyValues(toMap)

  /**
   * @return a [[Map hashMap representation]] of the row
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
    } map { case (name, (fmd, value_?)) => models.Field(name, fmd, value_?) }
  }

}