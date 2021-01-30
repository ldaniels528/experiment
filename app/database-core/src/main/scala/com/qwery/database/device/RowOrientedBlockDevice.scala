package com.qwery.database.device

import com.qwery.database.ROWID

/**
 * Row-Oriented Block Device
 */
trait RowOrientedBlockDevice extends BlockDevice {

  def fromOffset(offset: ROWID): ROWID = (offset / recordSize) + ((offset % recordSize) min 1L)

  def toOffset(rowID: ROWID): ROWID = rowID * recordSize

  def toOffset(rowID: ROWID, columnID: Int): ROWID = toOffset(rowID) + columnOffsets(columnID)

}
