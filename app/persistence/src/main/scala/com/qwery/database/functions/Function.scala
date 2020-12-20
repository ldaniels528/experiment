package com.qwery.database.functions

import com.qwery.database.ColumnTypes.ColumnType
import com.qwery.database.KeyValues

/**
 * Represents a SQL Function
 */
trait Function {

  def name: String

  def returnType: ColumnType

}
