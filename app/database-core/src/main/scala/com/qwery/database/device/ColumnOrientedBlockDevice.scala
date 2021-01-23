package com.qwery.database.device

import com.qwery.database.{Column, RECORD_ID, ROWID}

/**
 * Column-Oriented Block Device
 */
trait ColumnOrientedBlockDevice extends BlockDevice {

  def toOffset(rowID: ROWID, column: Column): RECORD_ID = rowID * column.maxPhysicalSize

}
