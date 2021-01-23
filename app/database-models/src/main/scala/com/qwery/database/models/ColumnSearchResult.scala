package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class ColumnSearchResult(databaseName: String, tableName: String, column: TableColumn) {
  override def toString: String = this.toJSON
}