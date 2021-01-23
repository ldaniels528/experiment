package com.qwery.database.functions

import com.qwery.database.KeyValues

/**
 * Represents an Aggregate Expression
 */
trait AggregateExpr {

  def name: String

  def collect: Any

  def append(keyValues: KeyValues): Unit

  def +=(keyValues: KeyValues): Unit = append(keyValues)

}
