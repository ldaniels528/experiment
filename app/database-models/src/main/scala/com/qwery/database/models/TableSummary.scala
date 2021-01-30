package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class TableSummary(tableName: String,
                        tableType: String,
                        description: Option[String],
                        lastModifiedTime: String,
                        href: Option[String] = None) {

  override def toString: String = this.toJSON

}
