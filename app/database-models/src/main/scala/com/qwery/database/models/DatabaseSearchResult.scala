package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class DatabaseSearchResult(databaseName: String) {
  override def toString: String = this.toJSON
}
