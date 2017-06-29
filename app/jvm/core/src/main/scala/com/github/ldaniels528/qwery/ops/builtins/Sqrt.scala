package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * SQRT(expression) function
  * @param expression the expression for which to compute the square root
  */
case class Sqrt(expression: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Double] = expression.getAsDouble(scope).map(Math.sqrt)
}
