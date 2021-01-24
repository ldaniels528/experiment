package com.qwery.database.server

import com.qwery.database.ColumnInfo

import scala.annotation.meta.field

case class StockQuoteWithDate(@(ColumnInfo@field)(maxSize = 8) symbol: String,
                              @(ColumnInfo@field)(maxSize = 8) exchange: String,
                              lastSale: Double,
                              lastSaleTime: java.util.Date)