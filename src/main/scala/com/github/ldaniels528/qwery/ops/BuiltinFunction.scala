package com.github.ldaniels528.qwery.ops

/**
  * Represents a function
  * @author lawrence.daniels@gmail.com
  */
abstract class BuiltinFunction(val isAggregatable: Boolean = false,
                               val isAggregateOnly: Boolean = false) extends Value {

  override def compare(that: Value, scope: Scope): Int = {
    evaluate(scope).map(Value.apply).map(_.compare(that, scope)) getOrElse -1
  }

  def update(scope: Scope): Unit

}
