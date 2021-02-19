package com.qwery.database.models

import com.qwery.database.{ColumnInfo, ROWID}

import scala.annotation.meta.field

case class StockQuoteWithID(@(ColumnInfo@field)(maxSize = 12) symbol: String,
                            @(ColumnInfo@field)(maxSize = 12) exchange: String,
                            lastSale: Double,
                            lastSaleTime: Long,
                            @(ColumnInfo@field)(isRowID = true) _id: ROWID = 0)