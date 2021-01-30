package com.qwery.database.files

import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.database.device.TableIndexRef

case class TableConfig(columns: Seq[TableColumn],
                       isColumnar: Boolean,
                       indices: Seq[TableIndexRef],
                       description: Option[String] = None) {
  override def toString: String = this.toJSON
}
