package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Aggregation, Expression, Scope}

/**
  * MIN(field) function
  * @param expression the expression for which to determine the minimum value
  */
case class Min(expression: Expression) extends InternalFunction with Aggregation {
  private var minimum: Option[Double] = None

  override def evaluate(scope: Scope): Option[Double] = {
    val result = minimum
    minimum = None
    result
  }

  override def update(scope: Scope): Unit = {
    val value = expression.getAsDouble(scope)
    if (minimum.isEmpty) minimum = value
    else if (value.nonEmpty) minimum = for (v <- value; max <- minimum) yield Math.min(max, v)
  }
}