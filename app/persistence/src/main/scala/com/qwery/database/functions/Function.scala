package com.qwery.database.functions

import com.qwery.database.ColumnTypes.ColumnType

/**
 * Represents a SQL Function
 */
trait Function {

  def name: String

  def execute: Any

  def returnType: ColumnType

}
