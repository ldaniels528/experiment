package com.qwery.database.device

import java.nio.ByteBuffer

import com.qwery.database.Codec.CodecByteBuffer
import com.qwery.database.{ROWID, RowMetadata}

/**
 * A binary row representation
 * @param id       the unique [[ROWID]]
 * @param metadata the [[RowMetadata]]
 * @param fields   the collection of [[ByteBuffer]]s representing the fields
 */
case class BinaryRow(id: ROWID, metadata: RowMetadata, fields: Seq[ByteBuffer])

/**
 * Binary Row Companion
 */
object BinaryRow {

  def apply(id: ROWID, buf: ByteBuffer)(implicit device: BlockDevice): BinaryRow = {
    BinaryRow(id, metadata = buf.getRowMetadata, fields = device.toFieldBuffers(buf))
  }

}