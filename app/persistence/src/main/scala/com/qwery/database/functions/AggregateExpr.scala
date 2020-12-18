package com.qwery.database.functions

import com.qwery.database.KeyValues

/**
 * Represents an Aggregate Expression
 */
trait AggregateExpr {

  def name: String

  def execute: Any

  def update(keyValues: KeyValues): Unit

}
