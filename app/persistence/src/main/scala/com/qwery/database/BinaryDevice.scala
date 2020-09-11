package com.qwery.database

import java.nio.ByteBuffer

/**
 * Represents a raw binary device
 */
trait BinaryDevice {

  /**
   * Closes the underlying file handle
   */
  def close(): Unit

  /**
   * Counts the number of rows matching the predicate
   * @param predicate the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  def countRows(predicate: RowMetaData => Boolean): Int = {
    val eof: ROWID = length
    var (rowID: ROWID, total) = (0, 0)
    while (rowID < eof) {
      if (predicate(readRowMetaData(rowID))) total += 1
      rowID += 1
    }
    total
  }

  def findRow(fromPos: ROWID = 0, forward: Boolean = true)(f: RowMetaData => Boolean): Option[ROWID] = {
    var rowID = fromPos
    if (forward) while (rowID < length && !f(readRowMetaData(rowID))) rowID += 1
    else while (rowID >= 0 && !f(readRowMetaData(rowID))) rowID -= 1
    if (rowID >= 0 && rowID < length) Some(rowID) else None
  }

  def firstIndexOption: Option[ROWID] = {
    var rowID: ROWID = 0
    val eof: ROWID = length
    while (rowID < eof && readRowMetaData(rowID).isDeleted) rowID += 1
    if (rowID < eof) Some(rowID) else None
  }

  def lastIndexOption: Option[ROWID] = {
    var rowID: ROWID = length - 1
    while (rowID >= 0 && readRowMetaData(rowID).isDeleted) rowID -= 1
    if (rowID >= 0) Some(rowID) else None
  }

  /**
   * @return the number of records in the file, including the deleted ones.
   *         The [[PersistentSeq.count count()]] method is probably the method you truly want.
   * @see [[PersistentSeq.count]]
   */
  def length: ROWID

  def readBlock(rowID: ROWID): ByteBuffer

  def readBlocks(rowID: ROWID, numberOfBlocks: Int = 1): Seq[(ROWID, ByteBuffer)] = {
    for {rowID <- rowID to rowID + numberOfBlocks} yield rowID -> readBlock(rowID)
  }

  def readByte(rowID: ROWID): Byte

  def readBytes(rowID: ROWID, numberOfBytes: Int, offset: Int = 0): ByteBuffer

  def readRowMetaData(rowID: ROWID): RowMetaData = RowMetaData.decode(readByte(rowID))

  /**
   * @return the record length in bytes
   */
  def recordSize: Int

  /**
   * Remove an item from the collection via its record offset
   * @param rowID the record offset
   */
  def remove(rowID: ROWID): Unit = writeRowMetaData(rowID, RowMetaData(isActive = false))

  def reverseInPlace(): Unit = {
    var (top: ROWID, bottom: ROWID) = (0, length - 1)
    while (bottom >= 0) {
      if (top != bottom) swap(top, bottom)
      bottom -= 1
      top += 1
    }
  }

  def reverseIteration: Iterator[(ROWID, ByteBuffer)] = new Iterator[(ROWID, ByteBuffer)] {
    private var item_? : Option[(ROWID, ByteBuffer)] = None
    private var offset: ROWID = BinaryDevice.this.length - 1

    override def hasNext: Boolean = {
      offset = findRow(fromPos = offset, forward = false)(_.isActive).getOrElse(-1)
      item_? = if (offset > -1) Some(offset -> readBlock(offset)) else None
      offset -= 1
      item_?.nonEmpty
    }

    override def next: (ROWID, ByteBuffer) = item_? match {
      case Some(item) => item_? = None; item
      case None =>
        throw new IllegalStateException("Iterator is empty")
    }
  }

  def shrinkTo(newSize: ROWID): Unit

  def swap(offset0: ROWID, offset1: ROWID): Unit = {
    val (block0, block1) = (readBlock(offset0), readBlock(offset1))
    writeBlock(offset0, block1)
    writeBlock(offset1, block0)
  }

  /**
   * Trims dead entries from of the collection
   * @return the new size of the file
   */
  def trim(): ROWID = {
    var rowID: ROWID = length - 1
    while (rowID >= 0 && readRowMetaData(rowID).isDeleted) rowID -= 1
    val newLength = rowID + 1
    shrinkTo(newLength)
    newLength
  }

  def writeBlock(rowID: ROWID, buf: ByteBuffer): Unit

  def writeBlocks(blocks: Seq[(ROWID, ByteBuffer)]): Unit = {
    blocks foreach { case (offset, buf) => writeBlock(offset, buf) }
  }

  def writeByte(rowID: ROWID, byte: Int): Unit

  def writeRowMetaData(rowID: ROWID, metaData: RowMetaData): Unit = writeByte(rowID, metaData.encode)

}
