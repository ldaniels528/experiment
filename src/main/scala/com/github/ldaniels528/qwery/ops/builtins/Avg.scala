package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Aggregation, Expression, Scope}

/**
  * AVG(field) function
  * @param expression the expression for which to compute the average
  */
case class Avg(expression: Expression) extends InternalFunction with Aggregation {
  private var total = 0d
  private var count = 0L

  override def evaluate(scope: Scope): Option[Double] = {
    val result = if (count > 0) Some(total / count) else None
    total = 0d
    count = 0L
    result
  }

  override def update(scope: Scope): Unit = {
    count += 1
    total += expression.getAsDouble(scope) getOrElse 0d
  }
}
