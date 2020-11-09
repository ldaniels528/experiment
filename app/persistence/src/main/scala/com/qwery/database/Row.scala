package com.qwery.database

/**
 * Represents a database row
 * @param rowID    the unique row identifier
 * @param metadata the [[RowMetadata row metadata]]
 * @param fields   the collection of [[Field fields]]
 */
case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field]) {

  def toTupleSet: TupleSet = TupleSet(toMap)

  def toMap: Map[String, Any] = Map(fields.flatMap(f => f.value.map(f.name -> _)): _*) + ("__id" -> rowID)

}