package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * Returns the sign of the argument as -1, 0, or 1, depending on whether X is negative, zero, or positive.
  * @param expression the given [[Expression expression]]
  */
case class Sign(expression: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Any] = {
    expression.getAsDouble(scope) map {
      case n if n < 0 => -1
      case n if n == 0 => 0
      case n if n > 0 => 1
    }
  }
}
