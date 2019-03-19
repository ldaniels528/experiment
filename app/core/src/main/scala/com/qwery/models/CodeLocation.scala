package com.qwery.models

/**
  * Represents a position within SQL code
  * @param lineNo   the line number
  * @param columnNo the column number
  */
case class CodeLocation(lineNo: Int, columnNo: Int) {

  override def toString: String = s"on line $lineNo at column $columnNo"

}
