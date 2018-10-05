package com.qwery.models.expressions

/**
  * If Expression
  * @param condition  the condition to evaluate
  * @param trueValue  the code to execute when the condition is satisfied
  * @param falseValue the code to execute when the condition is not satisfied
  */
case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends Expression