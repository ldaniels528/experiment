package com.qwery.database.models

import com.qwery.database.ROWID
import com.qwery.models.EntityRef

case class TableMetrics(ref: EntityRef,
                        columns: Seq[Column],
                        physicalSize: Option[Long],
                        recordSize: Int,
                        rows: ROWID)