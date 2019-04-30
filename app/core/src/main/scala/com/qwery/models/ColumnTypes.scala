package com.qwery.models

/**
  * Enumeration of Column Types
  * @author lawrence.daniels@gmail.com
  */
object ColumnTypes extends Enumeration {
  type ColumnType = Value
  val ARRAY, BINARY, BOOLEAN, BYTE, CHAR, DATE, DOUBLE, FLOAT, INTEGER, LONG, SHORT, STRING, TIMESTAMP, UUID: ColumnType = Value

}

