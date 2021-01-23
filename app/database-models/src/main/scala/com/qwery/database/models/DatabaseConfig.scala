package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion
import com.qwery.models.TypeAsEnum

case class DatabaseConfig(types: Seq[TypeAsEnum]) {
  override def toString: String = this.toJSON
}
