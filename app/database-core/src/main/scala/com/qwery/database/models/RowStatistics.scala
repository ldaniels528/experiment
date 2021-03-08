package com.qwery.database.models

import com.qwery.database.ROWID

case class RowStatistics(active: ROWID, compressed: ROWID, deleted: ROWID, encrypted: ROWID, locked: ROWID, replicated: ROWID) {
  def +(that: RowStatistics): RowStatistics = that.copy(
    active = that.active + active,
    compressed = that.compressed + compressed,
    deleted = that.deleted + deleted,
    encrypted = that.encrypted + encrypted,
    locked = that.locked + locked,
    replicated = that.replicated + replicated
  )
}
