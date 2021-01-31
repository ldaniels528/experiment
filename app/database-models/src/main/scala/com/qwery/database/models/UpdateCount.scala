package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class UpdateCount(count: Long, __id: Option[Long] = None) {
  override def toString: String = this.toJSON
}

object UpdateCount {
  def apply(response: Boolean): UpdateCount = UpdateCount(count = if(response) 1 else 0)
}