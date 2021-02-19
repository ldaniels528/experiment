package com.qwery.database.functions

import com.qwery.database.models.ColumnTypes.ColumnType
import com.qwery.database.models.KeyValues

/**
 * Represents a SQL Function
 */
trait Function {

  def name: String

  def returnType: ColumnType

}
