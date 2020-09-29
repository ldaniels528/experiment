package com.qwery.database

case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field])
