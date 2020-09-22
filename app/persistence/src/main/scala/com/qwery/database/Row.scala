package com.qwery.database

import com.qwery.database.Field

case class Row(rowID: ROWID, metadata: RowMetadata, fields: Seq[Field])
