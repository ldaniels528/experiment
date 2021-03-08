package com.qwery.database.models

case class UpdateCount(count: Long, __id: Option[Long] = None)

object UpdateCount {

  def apply(response: Boolean): UpdateCount = UpdateCount(count = if(response) 1 else 0)

}