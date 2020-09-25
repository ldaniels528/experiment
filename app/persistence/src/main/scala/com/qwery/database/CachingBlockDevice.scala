package com.qwery.database

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

import scala.collection.mutable

/**
 * Caching Block Device
 * @param host the host [[BlockDevice device]]
 */
class CachingBlockDevice(host: BlockDevice) extends BlockDevice {
  private val cache = mutable.Map[ROWID, Array[Byte]]()

  override def close(): Unit = {
    cache.clear()
    host.close()
  }

  override def columns: Seq[Column] = host.columns

  override def length: ROWID = host.length

  override def readBlock(rowID: ROWID): ByteBuffer = wrap(cache.getOrElseUpdate(rowID, host.readBlock(rowID).array()))

  override def readRowMetaData(rowID: ROWID): RowMetadata = host.readRowMetaData(rowID)

  override def readBytes(rowID: ROWID, numberOfBytes: ROWID, offset: ROWID): ByteBuffer = {
    //cache.remove(rowID)
    host.readBytes(rowID, numberOfBytes, offset)
  }

  override def shrinkTo(newSize: ROWID): Unit = {
    cache.clear()
    host.shrinkTo(newSize)
  }

  override def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit = {
    cache(rowID) = buf.array()
    host.writeBlock(rowID, buf)
  }

  override def writeRowMetaData(rowID: ROWID, metadata: RowMetadata): Unit = {
    cache.remove(rowID)
    host.writeRowMetaData(rowID, metadata)
  }

}
