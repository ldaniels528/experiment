package com.qwery.models.expressions

/**
  * Returns expr2 if expr1 is null, or expr1 otherwise.
  * @example ifnull(expr1, expr2)
  */
case class IfNull(condition: Condition, field2: Expression) extends Expression