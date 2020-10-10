package com.qwery.database
package jdbc

import java.nio.ByteBuffer

import com.qwery.database.Codec.CodecByteBuffer

/**
 * Qwery JDBC Row ID
 * @param __id the unique row ID
 */
case class JDBCRowId(__id: ROWID) extends java.sql.RowId {
  override def getBytes: Array[Byte] = {
    val buf = ByteBuffer.allocate(ROW_ID_BYTES).putRowID(__id)
    buf.flip()
    buf.array()
  }
}
