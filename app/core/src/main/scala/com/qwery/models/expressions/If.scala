package com.qwery.models.expressions

/**
  * If expr1 evaluates to true, then returns expr2; otherwise returns expr3.
  * @example if(expr1, expr2, expr3)
  */
case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends Expression