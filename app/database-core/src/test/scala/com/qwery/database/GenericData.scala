package com.qwery.database

import scala.annotation.meta.field

case class GenericData(@(ColumnInfo@field)(maxSize = 5) idValue: String,
                       @(ColumnInfo@field)(maxSize = 5) idType: String,
                       responseTime: Int,
                       reportDate: Long,
                       _id: Long = 0L)
