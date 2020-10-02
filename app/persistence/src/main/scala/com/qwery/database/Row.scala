package com.qwery.database

import com.qwery.database.server.TupleSet

case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  def toMap: TupleSet = Map("__id" -> rowID) ++ Map((for {field <- fields; value <- field.value} yield field.name -> value): _*)

}