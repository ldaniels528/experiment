package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Aggregation, Expression, Scope}

/**
  * MAX(field) function
  * @param expression the expression for which to determine the maximum value
  */
case class Max(expression: Expression) extends InternalFunction1 with Aggregation {
  private var maximum: Option[Double] = None

  override def evaluate(scope: Scope): Option[Double] = {
    val result = maximum
    maximum = None
    result
  }

  override def update(scope: Scope): Unit = {
    val value = expression.getAsDouble(scope)
    if (maximum.isEmpty) maximum = value
    else if (value.nonEmpty) maximum = for (v <- value; max <- maximum) yield Math.max(max, v)
  }
}