package com.qwery.database.models

case class UpdateCount(count: Long, __id: Option[Long] = None)

object UpdateCount {
  import com.qwery.database.models.DatabaseJsonProtocol._
  import spray.json._
  implicit val updateCount: RootJsonFormat[UpdateCount] = jsonFormat2(UpdateCount.apply)

  def apply(response: Boolean): UpdateCount = UpdateCount(count = if(response) 1 else 0)

}