package com.qwery.database.device

import com.qwery.database.{MathUtilsLong, RECORD_ID, ROWID}

/**
 * Row-Oriented Block Device
 */
trait RowOrientedBlockDevice extends BlockDevice {

  def fromOffset(offset: RECORD_ID): ROWID = {
    ((offset / recordSize) + Math.min(1, offset % recordSize)).toRowID
  }

  def toOffset(rowID: ROWID): RECORD_ID = rowID * recordSize

  def toOffset(rowID: ROWID, columnID: Int): RECORD_ID = toOffset(rowID) + columnOffsets(columnID)

}
