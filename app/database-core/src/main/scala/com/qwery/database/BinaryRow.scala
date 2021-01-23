package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.device.BlockDevice
import com.qwery.database.types.QxAny

/**
 * A binary row representation
 * @param id       the row's unique [[ROWID row identifier]]
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[ByteBuffer]]s representing the fields
 * @see [[Row]]
 */
case class BinaryRow(id: ROWID, metadata: RowMetadata = RowMetadata(), fields: Seq[ByteBuffer]) {

  def toFields(implicit device: BlockDevice): Seq[Field] = {
    device.columns zip fields map { case (column, fieldBuf) =>
      val (fmd, typedValue) = QxAny.decode(column, fieldBuf)
      Field(column.name, fmd, typedValue)
    }
  }

  /**
   * Returns the decode row representation of this encoded row
   * @param device the implicit [[BlockDevice]]
   * @return a new [[Row row]]
   */
  def toRow(implicit device: BlockDevice): Row = Row(id, metadata, fields = toFields)

  def toRowBuffer(implicit device: BlockDevice): ByteBuffer = {
    val buf = allocate(device.recordSize)
    buf.putRowMetadata(metadata)
    fields zip device.columnOffsets foreach { case (fieldBuf, offset) =>
      buf.position(offset)
      buf.put(fieldBuf)
    }
    buf.flip()
    buf
  }

}

/**
 * Binary Row Companion
 */
object BinaryRow {

  /**
   * Creates a new binary row
   * @param id     the row's unique [[ROWID row identifier]]
   * @param buf    the buffer containing the data for the row
   * @param device the implicit [[BlockDevice]]
   * @return a new [[BinaryRow binary row]]
   */
  def apply(id: ROWID, buf: ByteBuffer)(implicit device: BlockDevice): BinaryRow = {
    BinaryRow(id, metadata = buf.getRowMetadata, fields = Row.toFieldBuffers(buf))
  }

}