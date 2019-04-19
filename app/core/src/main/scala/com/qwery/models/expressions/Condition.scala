package com.qwery.models.expressions

/**
  * Base class for all conditional expressions
  * @author lawrence.daniels@gmail.com
  */
sealed trait Condition extends Expression

/**
  * Represents a conditional expression
  * @param expr0  the left-side [[Expression]]
  * @param expr1  the right-side [[Expression]]
  * @param codeOp the Spark operator (e.g. "!==")
  * @param sqlOp  the SQL operator (e.g. "<>")
  */
case class ConditionalOp(expr0: Expression, expr1: Expression, codeOp: String, sqlOp: String) extends Condition

/**
  * ConditionalOp Companion
  */
object ConditionalOp {
  def apply(expr0: Expression, expr1: Expression, op: String) = new ConditionalOp(expr0, expr1, op, op)
}

/**
  * SQL AND expression
  * @param a the left-value
  * @param b the right-value
  */
case class AND(a: Condition, b: Condition) extends Condition

/**
  * SQL Between clause
  * @param expr the host [[Expression expression]]
  * @param from the lower bound [[Expression expression]]
  * @param to   the upper bound [[Expression expression]]
  */
case class Between(expr: Expression, from: Expression, to: Expression) extends Condition

/**
  * If expr1 evaluates to true, then returns expr2; otherwise returns expr3.
  * @example if(expr1, expr2, expr3)
  */
case class If(condition: Condition, trueValue: Expression, falseValue: Expression) extends Expression

case class IsNotNull(expr: Expression) extends Condition

case class IsNull(expr: Expression) extends Condition

case class LIKE(a: Expression, b: Expression) extends Condition

case class NOT(condition: Condition) extends Condition

case class OR(a: Condition, b: Condition) extends Condition

case class RLIKE(a: Expression, b: Expression) extends Condition