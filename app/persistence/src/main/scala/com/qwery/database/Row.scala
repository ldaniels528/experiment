package com.qwery.database

import com.qwery.database.Field

case class Row(rowID: ROWID, metadata: RowMetaData, fields: Seq[Field])
