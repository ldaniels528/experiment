package com.qwery.database.files

import com.qwery.database.JSONSupport.JSONProductConversion

case class LoadMetrics(records: Long, ingestTime: Double, recordsPerSec: Double) {
  override def toString: String = this.toJSON
}
