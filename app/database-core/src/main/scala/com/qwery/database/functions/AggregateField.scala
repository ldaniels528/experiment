package com.qwery.database.functions

import com.qwery.database.models.KeyValues

/**
 * Represents an Aggregate Field
 * @param name    the output name of the result
 * @param srcName the name of the source fields
 */
case class AggregateField(name: String, srcName: String) extends AggregateExpr {
  private var value: Any = _

  override def collect: Any = value

  override def append(keyValues: KeyValues): Unit = value = keyValues.get(srcName).orNull

}
