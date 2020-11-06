package com.qwery.database

/**
 * Represents a database row
 * @param rowID    the unique row identifier
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  def toMap: TupleSet = TupleSet("__id" -> rowID) ++ TupleSet((for {field <- fields; value <- field.value} yield field.name -> value): _*)

}