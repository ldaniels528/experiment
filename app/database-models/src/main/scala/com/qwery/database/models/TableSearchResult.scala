package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class TableSearchResult(databaseName: String, tableName: String) {
  override def toString: String = this.toJSON
}
