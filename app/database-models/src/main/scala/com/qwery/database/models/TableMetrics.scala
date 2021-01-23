package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.database.ROWID

case class TableMetrics(databaseName: String,
                        tableName: String,
                        columns: Seq[TableColumn],
                        physicalSize: Option[Long],
                        recordSize: Int,
                        rows: ROWID) {
  override def toString: String = this.toJSON
}
