package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class DatabaseSummary(databaseName: String, tables: Seq[TableSummary]) {
  override def toString: String = this.toJSON
}