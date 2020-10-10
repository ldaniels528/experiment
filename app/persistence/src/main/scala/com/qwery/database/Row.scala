package com.qwery.database

import com.qwery.database.server.TupleSet

/**
 * Represents a database row
 * @param rowID    the unique row identifier
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  def toMap: TupleSet = Map("__id" -> rowID) ++ Map((for {field <- fields; value <- field.value} yield field.name -> value): _*)

}