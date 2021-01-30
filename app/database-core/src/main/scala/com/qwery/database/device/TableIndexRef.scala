package com.qwery.database.device

import com.qwery.database.JSONSupport.JSONProductConversion

/**
  * Represents a reference to a Table Index
  * @param databaseName    the name of the database
  * @param tableName       the name of the host table
  * @param indexColumnName the name of the index column
  */
case class TableIndexRef(databaseName: String, tableName: String, indexColumnName: String) {
  override def toString: String = this.toJSON
}
