package com.qwery.database.models

import com.qwery.database.JSONSupport.JSONProductConversion

case class UpdateCount(count: Int, __id: Option[Int] = None) {
  override def toString: String = this.toJSON
}