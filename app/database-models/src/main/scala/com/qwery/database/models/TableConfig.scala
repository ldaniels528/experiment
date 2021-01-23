package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class TableConfig(columns: Seq[TableColumn],
                       isColumnar: Boolean,
                       indices: Seq[TableIndexRef],
                       description: Option[String] = None) {
  override def toString: String = this.toJSON
}
