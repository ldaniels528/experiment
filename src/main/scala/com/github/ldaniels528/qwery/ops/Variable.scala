package com.github.ldaniels528.qwery.ops

/**
  * Represents a variable
  */
class Variable(initialValue: Option[Any] = None) {
  private var value: Option[Any] = initialValue

  def get: Option[Any] = value

  def set(scope: Scope, expression: Expression): Unit = {
    value = expression.evaluate(scope)
  }

}
