package com.qwery.models.expressions

/**
  * Represents a conditional expression
  * @author lawrence.daniels@gmail.com
  */
sealed trait Condition

case class AND(a: Condition, b: Condition) extends Condition

case class EQ(a: Expression, b: Expression) extends Condition

case class GE(a: Expression, b: Expression) extends Condition

case class GT(a: Expression, b: Expression) extends Condition

case class IsNotNull(expr: Expression) extends Condition

case class IsNull(expr: Expression) extends Condition

case class LE(a: Expression, b: Expression) extends Condition

case class LIKE(a: Expression, b: Expression) extends Condition

case class LT(a: Expression, b: Expression) extends Condition

case class NE(a: Expression, b: Expression) extends Condition

case class NOT(condition: Condition) extends Condition

case class OR(a: Condition, b: Condition) extends Condition

case class RLIKE(a: Expression, b: Expression) extends Condition