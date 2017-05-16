package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * LEN(field) function
  * @param expression the expression for which to compute the length
  */
case class Len(expression: Expression) extends InternalFunction {
  override def evaluate(scope: Scope): Option[Int] = expression.getAsString(scope).map(_.length)
}
