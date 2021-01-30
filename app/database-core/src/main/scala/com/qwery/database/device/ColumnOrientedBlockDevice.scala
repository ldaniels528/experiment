package com.qwery.database.device

import com.qwery.database.{Column, ROWID}

/**
 * Column-Oriented Block Device
 */
trait ColumnOrientedBlockDevice extends BlockDevice {

  def toOffset(rowID: ROWID, column: Column): ROWID = rowID * column.maxPhysicalSize

}
