package com.github.ldaniels528.qwery.ops

/**
  * Represents an internal function
  * @author lawrence.daniels@gmail.com
  */
abstract class InternalFunction(val isAggregatable: Boolean = false,
                                val isAggregateOnly: Boolean = false)
  extends Expression {

  def update(scope: Scope): Unit

}
