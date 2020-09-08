package com.qwery.database

import scala.annotation.meta.field

case class StockQuoteWithID(@(ColumnInfo@field)(maxSize = 12) symbol: String,
                            @(ColumnInfo@field)(maxSize = 12) exchange: String,
                            lastSale: Double,
                            lastSaleTime: Long,
                            _id: URID = 0)