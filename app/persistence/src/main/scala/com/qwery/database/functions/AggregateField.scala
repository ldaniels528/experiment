package com.qwery.database.functions

import com.qwery.database.KeyValues

/**
 * Represents an Aggregate Field
 * @param name    the output name of the result
 * @param srcName the name of the source fields
 */
case class AggregateField(name: String, srcName: String) extends AggregateExpr {
  private var value: Any = _

  override def execute: Any = value

  override def update(keyValues: KeyValues): Unit = value = keyValues.get(srcName).orNull

}
