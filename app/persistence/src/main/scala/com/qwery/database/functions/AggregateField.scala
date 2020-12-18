package com.qwery.database.functions

import com.qwery.database.KeyValues

/**
 * Represents an Aggregate Field
 * @param name the output name of the result
 */
case class AggregateField(name: String) extends AggregateExpr {
  private var value: Any = _

  override def execute: Any = value

  override def update(keyValues: KeyValues): Unit = value = keyValues.get(name).orNull

}
