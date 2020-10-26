package com.qwery.database.types

import com.qwery.database.ColumnTypes.ColumnType

case class ArrayBlock(`type`: ColumnType, items: Seq[QxAny]) {

  def length: Int = items.length

}
