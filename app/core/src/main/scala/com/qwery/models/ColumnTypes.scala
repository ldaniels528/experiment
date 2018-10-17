package com.qwery.models

/**
  * Enumeration of Column Types
  * @author lawrence.daniels@gmail.com
  */
object ColumnTypes extends Enumeration {
  type ColumnType = Value
  val ARRAY, BIGINT, BINARY, BOOLEAN, DATE, DATETIME, DOUBLE, INT, INTEGER, LONG, STRING, TIMESTAMP, UUID: ColumnType = Value

}

