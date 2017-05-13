package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Expression, Scope}

/**
  * TRIM(field) function
  * @param expression the (string) expression to trim
  */
case class Trim(expression: Expression) extends InternalFunction1 {
  override def evaluate(scope: Scope): Option[String] = expression.getAsString(scope).map(_.trim)
}
