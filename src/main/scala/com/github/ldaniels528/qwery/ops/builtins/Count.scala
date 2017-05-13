package com.github.ldaniels528.qwery.ops.builtins

import com.github.ldaniels528.qwery.SyntaxException
import com.github.ldaniels528.qwery.ops.{Aggregation, Expression, Field, Scope}

/**
  * COUNT(field) function
  * @param expression the field to count
  */
case class Count(expression: Expression) extends InternalFunction1 with Aggregation {
  private var count = 0L
  private val field = expression match {
    case f: Field => f
    case _ => throw new SyntaxException(s"$expression is not a field")
  }

  override def evaluate(scope: Scope): Option[Long] = {
    val returnValue = Some(count)
    count = 0L
    returnValue
  }

  override def update(scope: Scope): Unit = if (field.name == "*" || scope.get(field.name).nonEmpty) count += 1
}
