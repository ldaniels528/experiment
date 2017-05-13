package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.ops.{Aggregation, Expression, Scope}

/**
  * SUM(field) function
  * @param expression the expression to sum
  */
case class Sum(expression: Expression) extends InternalFunction1 with Aggregation {
  private var total: Option[Double] = None

  override def evaluate(scope: Scope): Option[Double] = {
    val result = total
    total = None
    result
  }

  override def update(scope: Scope): Unit = {
    val value = expression.getAsDouble(scope)
    if (total.isEmpty) total = value
    else if (value.nonEmpty) total = for {t <- total; v <- value} yield t + v
  }
}